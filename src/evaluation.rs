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
    ) -> Option<(PriceLookup, OutcomeCheckEvent)> {
        let history = self.prices.get(&request.symbol)?;
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
            })?;

        let future = history
            .iter()
            .find(|point| point.time_ms >= target_time_ms)
            .cloned()?;

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

        Some((
            PriceLookup { anchor, future },
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
                Some((lookup, outcome)) => {
                    debug!(
                        event_id = %request.event_id,
                        horizon_s,
                        anchor_time_ms = lookup.anchor.time_ms,
                        future_time_ms = lookup.future.time_ms,
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
                None => warn!(
                    event_id = %request.event_id,
                    symbol = %request.symbol,
                    horizon_s,
                    event_time_ms = request.event_time_ms,
                    target_time_ms,
                    anchor_lookup = "missing",
                    future_lookup = "missing_or_before_target",
                    "insufficient price history for outcome evaluation"
                ),
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
