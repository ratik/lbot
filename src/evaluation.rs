use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use tracing::{debug, info, warn};

use crate::{
    config::AppConfig,
    persistence::PersistenceHandle,
    types::{Direction, OutcomeCheckEvent},
};

const MAX_FUTURE_STALENESS_MS: i64 = 2_000;

#[derive(Debug, Clone)]
pub struct EvaluationRequest {
    pub event_id: String,
    pub symbol: String,
    pub direction: Direction,
    pub event_time_ms: i64,
    pub reference_price: f64,
}

#[derive(Debug, Clone)]
pub struct TimedPrice {
    pub time_ms: i64,
    pub price: f64,
}

#[derive(Debug, Clone)]
struct PriceLookup {
    anchor: TimedPrice,
    future: TimedPrice,
    future_lookup_mode: FutureLookupMode,
}

#[derive(Debug, Clone, Copy)]
enum FutureLookupMode {
    ExactOrAfterTarget,
    FallbackBeforeTarget,
}

impl FutureLookupMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::ExactOrAfterTarget => "exact_or_after_target",
            Self::FallbackBeforeTarget => "fallback_before_target",
        }
    }
}

#[derive(Debug, Clone)]
struct LookupDiagnostics {
    anchor_lookup: &'static str,
    future_lookup: &'static str,
    nearest_before_target_ts: Option<i64>,
    nearest_after_target_ts: Option<i64>,
    delta_before_ms: Option<i64>,
    delta_after_ms: Option<i64>,
}

#[derive(Debug, Clone)]
enum EvaluationFailure {
    MissingSymbolHistory,
    MissingAnchor {
        nearest_after_event_ts: Option<i64>,
    },
    MissingFuture {
        nearest_before_target_ts: Option<i64>,
        nearest_after_target_ts: Option<i64>,
        delta_before_ms: Option<i64>,
        delta_after_ms: Option<i64>,
    },
}

impl EvaluationFailure {
    fn diagnostics(&self) -> LookupDiagnostics {
        match self {
            Self::MissingSymbolHistory => LookupDiagnostics {
                anchor_lookup: "missing",
                future_lookup: "not_attempted",
                nearest_before_target_ts: None,
                nearest_after_target_ts: None,
                delta_before_ms: None,
                delta_after_ms: None,
            },
            Self::MissingAnchor {
                nearest_after_event_ts,
            } => LookupDiagnostics {
                anchor_lookup: "missing",
                future_lookup: "not_attempted",
                nearest_before_target_ts: None,
                nearest_after_target_ts: *nearest_after_event_ts,
                delta_before_ms: None,
                delta_after_ms: None,
            },
            Self::MissingFuture {
                nearest_before_target_ts,
                nearest_after_target_ts,
                delta_before_ms,
                delta_after_ms,
            } => LookupDiagnostics {
                anchor_lookup: "ok",
                future_lookup: "missing_at_or_after_target",
                nearest_before_target_ts: *nearest_before_target_ts,
                nearest_after_target_ts: *nearest_after_target_ts,
                delta_before_ms: *delta_before_ms,
                delta_after_ms: *delta_after_ms,
            },
        }
    }
}

#[derive(Debug, Default)]
pub struct MarketHistory {
    prices: HashMap<String, VecDeque<TimedPrice>>,
}

pub type SharedMarketHistory = Arc<Mutex<MarketHistory>>;

impl MarketHistory {
    pub fn record(&mut self, symbol: &str, time_ms: i64, price: f64) {
        let history = self.prices.entry(symbol.to_string()).or_default();
        history.push_back(TimedPrice { time_ms, price });
        let cutoff = time_ms - 20 * 60 * 1000;
        while history
            .front()
            .map(|point| point.time_ms < cutoff)
            .unwrap_or(false)
        {
            history.pop_front();
        }
    }

    fn evaluate_with_lookup(
        &self,
        request: &EvaluationRequest,
        horizon_s: u64,
        success_threshold_bps: f64,
    ) -> Result<(PriceLookup, OutcomeCheckEvent), EvaluationFailure> {
        let history = self
            .prices
            .get(&request.symbol)
            .ok_or(EvaluationFailure::MissingSymbolHistory)?;
        let target_time_ms = request.event_time_ms + (horizon_s as i64) * 1000;

        // Allow a pre-event anchor because the triggering quote may have been recorded
        // slightly before the signal timestamp assigned from the processed market data.
        let anchor = history
            .iter()
            .rev()
            .find(|point| point.time_ms <= request.event_time_ms)
            .cloned()
            .or_else(|| {
                history
                    .iter()
                    .find(|point| point.time_ms > request.event_time_ms)
                    .cloned()
            })
            .ok_or_else(|| EvaluationFailure::MissingAnchor {
                nearest_after_event_ts: history
                    .iter()
                    .find(|point| point.time_ms > request.event_time_ms)
                    .map(|point| point.time_ms),
            })?;

        let first_at_or_after_target = history
            .iter()
            .find(|point| point.time_ms >= target_time_ms)
            .cloned();
        let latest_before_target = history
            .iter()
            .rev()
            .find(|point| point.time_ms < target_time_ms)
            .cloned();

        let (future, future_lookup_mode) = match first_at_or_after_target {
            Some(point) => (point, FutureLookupMode::ExactOrAfterTarget),
            None => match latest_before_target {
                Some(point) if target_time_ms - point.time_ms <= MAX_FUTURE_STALENESS_MS => {
                    (point, FutureLookupMode::FallbackBeforeTarget)
                }
                maybe_point => {
                    return Err(EvaluationFailure::MissingFuture {
                        nearest_before_target_ts: maybe_point.as_ref().map(|point| point.time_ms),
                        nearest_after_target_ts: None,
                        delta_before_ms: maybe_point
                            .as_ref()
                            .map(|point| target_time_ms - point.time_ms),
                        delta_after_ms: None,
                    });
                }
            },
        };

        let mut max_price = future.price;
        let mut min_price = future.price;
        let mut prices = vec![anchor.price];

        for point in history
            .iter()
            .filter(|point| point.time_ms > anchor.time_ms && point.time_ms <= future.time_ms)
        {
            max_price = max_price.max(point.price);
            min_price = min_price.min(point.price);
            prices.push(point.price);
        }

        let return_bps = bps_move(request.reference_price, future.price, request.direction);
        let max_favorable_bps = match request.direction {
            Direction::Long => bps_move(request.reference_price, min_price, Direction::Long),
            Direction::Short => bps_move(request.reference_price, max_price, Direction::Short),
        };
        let max_adverse_bps = match request.direction {
            Direction::Long => bps_move(request.reference_price, max_price, Direction::Short),
            Direction::Short => bps_move(request.reference_price, min_price, Direction::Long),
        };
        let realized_vol_after = realized_vol(&prices);

        Ok((
            PriceLookup {
                anchor,
                future,
                future_lookup_mode,
            },
            OutcomeCheckEvent {
                event_id: request.event_id.clone(),
                horizon_s,
                return_bps,
                max_favorable_bps,
                max_adverse_bps,
                realized_vol_after,
                success_flag: max_favorable_bps >= success_threshold_bps,
            },
        ))
    }
}

pub fn spawn_evaluation_tasks(
    config: Arc<AppConfig>,
    persistence: PersistenceHandle,
    market_history: SharedMarketHistory,
    request: EvaluationRequest,
) {
    let symbol = request.symbol.clone();
    for horizon_s in config.evaluation.horizons_s.clone() {
        let config = Arc::clone(&config);
        let persistence = persistence.clone();
        let market_history = Arc::clone(&market_history);
        let request = request.clone();
        let symbol = symbol.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(horizon_s)).await;
            let success_threshold_bps = config.symbol_success_threshold_bps(&symbol);
            let target_time_ms = request.event_time_ms + (horizon_s as i64) * 1000;
            let outcome = {
                let history = market_history.lock().expect("market history lock poisoned");
                history.evaluate_with_lookup(&request, horizon_s, success_threshold_bps)
            };
            match outcome {
                Ok((lookup, outcome)) => {
                    debug!(
                        event_id = %request.event_id,
                        symbol = %request.symbol,
                        horizon_s,
                        anchor_time_ms = lookup.anchor.time_ms,
                        future_time_ms = lookup.future.time_ms,
                        target_time_ms,
                        future_lookup_mode = lookup.future_lookup_mode.as_str(),
                        "outcome evaluation lookup succeeded"
                    );
                    if let Err(err) = persistence.send_outcome(outcome.clone()) {
                        warn!(error = %err, event_id = %outcome.event_id, "failed to persist outcome");
                    } else {
                        info!(
                            event_id = %outcome.event_id,
                            horizon_s = outcome.horizon_s,
                            success_flag = outcome.success_flag,
                            "outcome evaluated"
                        );
                    }
                }
                Err(failure) => {
                    let diagnostics = failure.diagnostics();
                    warn!(
                        event_id = %request.event_id,
                        symbol = %request.symbol,
                        horizon_s,
                        event_time_ms = request.event_time_ms,
                        target_time_ms,
                        anchor_lookup = diagnostics.anchor_lookup,
                        future_lookup = diagnostics.future_lookup,
                        nearest_before_target_ts = diagnostics.nearest_before_target_ts,
                        nearest_after_target_ts = diagnostics.nearest_after_target_ts,
                        delta_before_ms = diagnostics.delta_before_ms,
                        delta_after_ms = diagnostics.delta_after_ms,
                        "insufficient price history for outcome evaluation"
                    );
                }
            }
        });
    }
}

fn bps_move(reference: f64, observed: f64, direction: Direction) -> f64 {
    if reference <= 0.0 {
        return 0.0;
    }
    let raw = ((observed / reference) - 1.0) * 10_000.0;
    match direction {
        Direction::Long => -raw,
        Direction::Short => raw,
    }
}

fn realized_vol(prices: &[f64]) -> f64 {
    if prices.len() < 2 {
        return 0.0;
    }
    let mut sum = 0.0;
    for pair in prices.windows(2) {
        let ret = (pair[1] / pair[0]) - 1.0;
        sum += ret * ret;
    }
    sum.sqrt()
}
