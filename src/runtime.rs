use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    config::AppConfig,
    evaluation::{EvaluationRequest, MarketHistory, SharedMarketHistory, spawn_evaluation_tasks},
    features::{FeatureSnapshot, compute_features},
    persistence::PersistenceHandle,
    scoring::{compute_market_confirmation, compute_venue_scores},
    signals::{SignalKey, SignalRegistry, SignalScope},
    sources,
    state::RollingState,
    types::{
        Direction, HeartbeatEvent, MarketEvent, MarketSignalEvent, Venue, VenueScoreSnapshot,
        VenueSignalEvent,
    },
};

#[derive(Debug, Default)]
struct BlockCounters {
    warmup: u64,
    directional_gate: u64,
    both_failed: u64,
    dual_pass_stronger_only: u64,
}

impl BlockCounters {
    fn note(&mut self, kind: BlockKind, symbol: &str, venue: Venue, detail: &str) {
        let counter = match kind {
            BlockKind::Warmup => &mut self.warmup,
            BlockKind::DirectionalGate => &mut self.directional_gate,
            BlockKind::BothFailed => &mut self.both_failed,
            BlockKind::DualPassStrongerOnly => &mut self.dual_pass_stronger_only,
        };
        *counter += 1;
        if *counter <= 5 || *counter % 100 == 0 {
            info!(
                block_kind = kind.as_str(),
                count = *counter,
                symbol,
                venue = venue.as_str(),
                detail,
                "venue signal path blocked or reduced"
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum BlockKind {
    Warmup,
    DirectionalGate,
    BothFailed,
    DualPassStrongerOnly,
}

impl BlockKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Warmup => "warmup",
            Self::DirectionalGate => "directional_gate",
            Self::BothFailed => "both_failed",
            Self::DualPassStrongerOnly => "dual_pass_stronger_only",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct WarmupStatus {
    eligible: bool,
    age_ms: i64,
    min_age_ms: i64,
    has_window_coverage: bool,
}

pub async fn run(config: AppConfig) -> Result<()> {
    let config = Arc::new(config);
    let persistence = PersistenceHandle::new(&config.sqlite_path)?;
    let (tx, mut rx) = mpsc::channel::<MarketEvent>(20_000);
    let _handles = sources::spawn_sources(&config, tx)?;
    let market_history: SharedMarketHistory = Arc::new(Mutex::new(MarketHistory::default()));
    spawn_heartbeat_task(persistence.clone());

    let mut venue_states: HashMap<(String, Venue), RollingState> = HashMap::new();
    let mut latest_scores: HashMap<(String, Venue), VenueScoreSnapshot> = HashMap::new();
    let mut signal_registry = SignalRegistry::default();
    let mut block_counters = BlockCounters::default();

    info!(
        symbols = ?config.symbols,
        venues = ?config.enabled_venues().iter().map(|venue| venue.as_str()).collect::<Vec<_>>(),
        sqlite_path = %config.sqlite_path,
        "startup complete"
    );

    while let Some(event) = rx.recv().await {
        let key = (event.symbol().to_string(), event.venue());
        let symbol = key.0.clone();
        let venue = key.1;

        let quote_recv_time_ms = match &event {
            MarketEvent::Quote(tick) => Some(tick.recv_time_ms),
            _ => None,
        };

        let features = {
            let state = venue_states.entry(key.clone()).or_insert_with(|| {
                info!(symbol = %symbol, venue = venue.as_str(), "per-symbol state initialized");
                RollingState::new(
                    symbol.clone(),
                    venue,
                    &config.feature_windows,
                    config.online_bucket_ms,
                )
            });

            match event {
                MarketEvent::Trade(tick) => state.apply_trade(tick),
                MarketEvent::Quote(tick) => state.apply_quote(tick),
            }

            compute_features(state, &config.feature_windows, &config.zscore_windows)
        };

        if let Some(recv_time_ms) = quote_recv_time_ms {
            if let Some(market_price) = blended_market_price(&symbol, &venue_states) {
                let mut history = market_history.lock().expect("market history lock poisoned");
                history.record(&symbol, recv_time_ms, market_price);
            }
        }

        if let Some(features) = features {
            let scores = compute_venue_scores(
                &features,
                &config.score_weights,
                config.min_directional_core,
                config.max_stress_amplifier,
            );
            let (price, spread_bps) = venue_states
                .get(&key)
                .map(|state| {
                    (
                        state.latest_mid().unwrap_or_default(),
                        state.latest_spread_bps().unwrap_or_default(),
                    )
                })
                .unwrap_or_default();
            let snapshot_time_ms = now_ms();
            let warmup = warmup_status(
                &config,
                venue_states.get(&key).expect("state exists"),
                snapshot_time_ms,
            );
            let snapshot = VenueScoreSnapshot {
                symbol: symbol.clone(),
                venue,
                event_time_ms: snapshot_time_ms,
                price,
                spread_bps,
                directional_core_long: scores.directional_core_long,
                directional_core_short: scores.directional_core_short,
                stress_long: scores.stress_long,
                stress_short: scores.stress_short,
                min_directional_core_pass_long: scores.min_directional_core_pass_long,
                min_directional_core_pass_short: scores.min_directional_core_pass_short,
                long_score: scores.long_score,
                short_score: scores.short_score,
            };
            if warmup.eligible {
                latest_scores.insert((symbol.clone(), venue), snapshot.clone());
            }
            emit_venue_signal_if_needed(
                &config,
                &persistence,
                &market_history,
                &mut signal_registry,
                &mut block_counters,
                &snapshot,
                &features,
                warmup,
            );

            let symbol_snapshots = latest_scores
                .values()
                .filter(|snapshot| snapshot.symbol == symbol)
                .cloned()
                .collect::<Vec<_>>();

            let market = compute_market_confirmation(
                &symbol,
                &symbol_snapshots,
                &config.score_weights,
                config.thresholds.venue_long_threshold,
                config.thresholds.venue_short_threshold,
            );

            emit_market_signal_if_needed(
                &config,
                &persistence,
                &market_history,
                &mut signal_registry,
                &symbol_snapshots,
                &market,
                &features,
            );
        }
    }

    Ok(())
}

fn emit_venue_signal_if_needed(
    config: &Arc<AppConfig>,
    persistence: &PersistenceHandle,
    market_history: &SharedMarketHistory,
    signal_registry: &mut SignalRegistry,
    block_counters: &mut BlockCounters,
    snapshot: &VenueScoreSnapshot,
    features: &FeatureSnapshot,
    warmup: WarmupStatus,
) {
    let now_ms = snapshot.event_time_ms;
    let return_1s_bps = features.return_1s * 10_000.0;
    let min_return_check =
        config.min_return_bps <= 0.0 || return_1s_bps.abs() >= config.min_return_bps;
    let long_directional_gate = features.return_1s_zscore
        <= -config.directional_gates.long_return_z_gate
        && features.signed_volume_1s_z <= -config.directional_gates.long_volume_z_gate
        && min_return_check;
    let short_directional_gate = features.return_1s_zscore
        >= config.directional_gates.short_return_z_gate
        && features.signed_volume_1s_z >= config.directional_gates.short_volume_z_gate
        && min_return_check;
    let feature_blob = serde_json::json!({
        "bucket_time_ms": features.bucket_time_ms,
        "warmup": {
            "eligible": warmup.eligible,
            "age_ms": warmup.age_ms,
            "min_age_ms": warmup.min_age_ms,
            "has_window_coverage": warmup.has_window_coverage,
        },
        "gates": {
            "long_directional_gate": long_directional_gate,
            "short_directional_gate": short_directional_gate,
            "long_return_z_gate": config.directional_gates.long_return_z_gate,
            "long_volume_z_gate": config.directional_gates.long_volume_z_gate,
            "short_return_z_gate": config.directional_gates.short_return_z_gate,
            "short_volume_z_gate": config.directional_gates.short_volume_z_gate,
            "min_return_bps": config.min_return_bps,
            "min_return_check": min_return_check,
        },
        "scores": {
            "directional_core_long": snapshot.directional_core_long,
            "directional_core_short": snapshot.directional_core_short,
            "stress_long": snapshot.stress_long,
            "stress_short": snapshot.stress_short,
            "min_directional_core": config.min_directional_core,
            "min_directional_core_pass_long": snapshot.min_directional_core_pass_long,
            "min_directional_core_pass_short": snapshot.min_directional_core_pass_short,
            "long_score": snapshot.long_score,
            "short_score": snapshot.short_score,
        },
        "features": features,
    });
    let feature_json = match serde_json::to_string(&feature_blob) {
        Ok(json) => json,
        Err(err) => {
            warn!(error = %err, "failed serializing venue features");
            return;
        }
    };

    if !warmup.eligible {
        block_counters.note(
            BlockKind::Warmup,
            &snapshot.symbol,
            snapshot.venue,
            "warmup incomplete",
        );
        return;
    }

    let long_candidate =
        long_directional_gate && snapshot.long_score >= config.thresholds.venue_long_threshold;
    let short_candidate =
        short_directional_gate && snapshot.short_score >= config.thresholds.venue_short_threshold;

    if !long_directional_gate || !short_directional_gate {
        block_counters.note(
            BlockKind::DirectionalGate,
            &snapshot.symbol,
            snapshot.venue,
            "directional gate blocked at least one side",
        );
    }

    let selected = match (long_candidate, short_candidate) {
        (false, false) => {
            block_counters.note(
                BlockKind::BothFailed,
                &snapshot.symbol,
                snapshot.venue,
                "both directional candidates failed threshold/gate",
            );
            None
        }
        (true, false) => Some((Direction::Long, snapshot.long_score)),
        (false, true) => Some((Direction::Short, snapshot.short_score)),
        (true, true) => {
            block_counters.note(
                BlockKind::DualPassStrongerOnly,
                &snapshot.symbol,
                snapshot.venue,
                "both directions passed; stronger side selected",
            );
            if snapshot.long_score >= snapshot.short_score {
                Some((Direction::Long, snapshot.long_score))
            } else {
                Some((Direction::Short, snapshot.short_score))
            }
        }
    };

    let Some((direction, score)) = selected else {
        return;
    };

    let threshold = match direction {
        Direction::Long => config.thresholds.venue_long_threshold,
        Direction::Short => config.thresholds.venue_short_threshold,
    };
    let gate = signal_registry.gate_mut(
        &snapshot.symbol,
        SignalKey {
            scope: SignalScope::Venue {
                venue: snapshot.venue,
            },
            direction,
        },
        threshold,
        config.thresholds.venue_rearm_ratio,
        (config.cooldowns.venue_event_s as i64) * 1000,
    );

    if gate.should_fire(score, now_ms) {
        let event = VenueSignalEvent {
            event_id: Uuid::new_v4().to_string(),
            event_time_ms: now_ms,
            symbol: snapshot.symbol.clone(),
            venue: snapshot.venue,
            direction,
            score,
            price: snapshot.price,
            spread_bps: snapshot.spread_bps,
            feature_json: feature_json.clone(),
            cooldown_state: "cooldown".to_string(),
        };

        if let Err(err) = persistence.send_venue_signal(event.clone()) {
            error!(error = %err, event_id = %event.event_id, "failed to persist venue signal");
            return;
        }

        info!(
            event = if direction == Direction::Long { "VENUE_LONG_LIQ_RISK" } else { "VENUE_SHORT_LIQ_RISK" },
            event_id = %event.event_id,
            symbol = %event.symbol,
            venue = event.venue.as_str(),
            direction = event.direction.as_str(),
            score = event.score,
            return_1s = features.return_1s,
            return_5s = features.return_5s,
            signed_volume_1s = features.signed_volume_1s,
            signed_volume_5s = features.signed_volume_5s,
            signed_volume_1s_z = features.signed_volume_1s_z,
            signed_volume_5s_z = features.signed_volume_5s_z,
            trade_count_1s_z = features.trade_count_1s_z,
            trade_count_5s_z = features.trade_count_5s_z,
            spread_bps = features.spread_bps_now,
            spread_bps_z = features.spread_bps_zscore,
            realized_vol_5s_z = features.realized_vol_5s_z,
            realized_vol_15s_z = features.realized_vol_15s_z,
            bucket_time_ms = features.bucket_time_ms,
            return_1s_zscore = features.return_1s_zscore,
            long_return_z_gate = config.directional_gates.long_return_z_gate,
            long_volume_z_gate = config.directional_gates.long_volume_z_gate,
            short_return_z_gate = config.directional_gates.short_return_z_gate,
            short_volume_z_gate = config.directional_gates.short_volume_z_gate,
            min_return_bps = config.min_return_bps,
            min_return_check,
            directional_core_long = snapshot.directional_core_long,
            directional_core_short = snapshot.directional_core_short,
            stress_long = snapshot.stress_long,
            stress_short = snapshot.stress_short,
            min_directional_core = config.min_directional_core,
            min_directional_core_pass_long = snapshot.min_directional_core_pass_long,
            min_directional_core_pass_short = snapshot.min_directional_core_pass_short,
            "venue signal emitted"
        );

        spawn_evaluation_tasks(
            Arc::clone(config),
            persistence.clone(),
            Arc::clone(market_history),
            EvaluationRequest {
                event_id: event.event_id,
                symbol: event.symbol,
                direction,
                event_time_ms: event.event_time_ms,
                reference_price: event.price,
            },
        );
    }
}

fn emit_market_signal_if_needed(
    config: &Arc<AppConfig>,
    persistence: &PersistenceHandle,
    market_history: &SharedMarketHistory,
    signal_registry: &mut SignalRegistry,
    symbol_snapshots: &[VenueScoreSnapshot],
    market: &crate::scoring::MarketConfirmation,
    features: &FeatureSnapshot,
) {
    let feature_json = match serde_json::to_string(&serde_json::json!({
        "market": market,
        "features": features,
        "venues": symbol_snapshots,
    })) {
        Ok(json) => json,
        Err(err) => {
            warn!(error = %err, "failed serializing market features");
            return;
        }
    };
    let now_ms = now_ms();

    for (direction, score, confirming, first, latest, spread_ms, threshold) in [
        (
            Direction::Long,
            market.long_score,
            &market.confirming_long,
            market.first_triggered_long,
            market.latest_triggered_long,
            market.long_spread_ms,
            config.thresholds.market_long_threshold,
        ),
        (
            Direction::Short,
            market.short_score,
            &market.confirming_short,
            market.first_triggered_short,
            market.latest_triggered_short,
            market.short_spread_ms,
            config.thresholds.market_short_threshold,
        ),
    ] {
        let confirmations_ok = confirming.len() >= config.thresholds.market_min_confirmations
            && confirming.iter().any(|venue| venue.is_major());
        if !confirmations_ok {
            continue;
        }

        let gate = signal_registry.gate_mut(
            &market.symbol,
            SignalKey {
                scope: SignalScope::Market,
                direction,
            },
            threshold,
            config.thresholds.market_rearm_ratio,
            (config.cooldowns.market_event_s as i64) * 1000,
        );

        if gate.should_fire(score, now_ms) {
            let event = MarketSignalEvent {
                event_id: Uuid::new_v4().to_string(),
                event_time_ms: now_ms,
                symbol: market.symbol.clone(),
                direction,
                combined_score: score,
                confirming_venues_json: serde_json::to_string(confirming)
                    .unwrap_or_else(|_| "[]".to_string()),
                first_triggered_venue: first
                    .or(latest)
                    .map(|venue| venue.as_str().to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                venue_spread_ms: spread_ms,
                feature_json: feature_json.clone(),
            };

            if let Err(err) = persistence.send_market_signal(event.clone()) {
                error!(error = %err, event_id = %event.event_id, "failed to persist market signal");
                continue;
            }

            info!(
                event = if direction == Direction::Long { "MARKET_LONG_LIQ_RISK" } else { "MARKET_SHORT_LIQ_RISK" },
                event_id = %event.event_id,
                symbol = %event.symbol,
                score = event.combined_score,
                confirmations = confirming.len(),
                "market signal emitted"
            );

            if let Some(reference_price) =
                blended_market_price_from_snapshots(&market.symbol, symbol_snapshots)
            {
                spawn_evaluation_tasks(
                    Arc::clone(config),
                    persistence.clone(),
                    Arc::clone(market_history),
                    EvaluationRequest {
                        event_id: event.event_id,
                        symbol: event.symbol,
                        direction,
                        event_time_ms: event.event_time_ms,
                        reference_price,
                    },
                );
            }
        }
    }
}

fn warmup_status(config: &AppConfig, state: &RollingState, now_ms: i64) -> WarmupStatus {
    let first_seen_ms = state.first_seen_ms().unwrap_or(now_ms);
    let age_ms = now_ms - first_seen_ms;
    let min_age_ms = (config.warmup_seconds as i64) * 1000;
    let required_window_ms = ((config.zscore_windows.seconds as i64) * 1000)
        .max((config.feature_windows.return_15s as i64) * 1000)
        .max((config.feature_windows.return_5s as i64) * 1000)
        .max((config.feature_windows.return_1s as i64) * 1000);
    let has_window_coverage = state.has_min_buckets_for(required_window_ms);

    WarmupStatus {
        eligible: age_ms >= min_age_ms && has_window_coverage,
        age_ms,
        min_age_ms,
        has_window_coverage,
    }
}

fn blended_market_price(
    symbol: &str,
    venue_states: &HashMap<(String, Venue), RollingState>,
) -> Option<f64> {
    let mut weighted_sum = 0.0;
    let mut total_weight = 0.0;

    for ((state_symbol, venue), state) in venue_states {
        if state_symbol != symbol {
            continue;
        }
        let price = match state.latest_mid() {
            Some(price) if price > 0.0 => price,
            _ => continue,
        };
        let weight = match venue {
            Venue::Binance => 1.0,
            Venue::Bybit => 0.8,
            Venue::Okx => 0.8,
            Venue::Dydx => 0.3,
        };
        weighted_sum += price * weight;
        total_weight += weight;
    }

    if total_weight > 0.0 {
        Some(weighted_sum / total_weight)
    } else {
        None
    }
}

fn blended_market_price_from_snapshots(
    symbol: &str,
    snapshots: &[VenueScoreSnapshot],
) -> Option<f64> {
    let mut weighted_sum = 0.0;
    let mut total_weight = 0.0;

    for snapshot in snapshots
        .iter()
        .filter(|snapshot| snapshot.symbol == symbol)
    {
        if snapshot.price <= 0.0 {
            continue;
        }
        let weight = match snapshot.venue {
            Venue::Binance => 1.0,
            Venue::Bybit => 0.8,
            Venue::Okx => 0.8,
            Venue::Dydx => 0.3,
        };
        weighted_sum += snapshot.price * weight;
        total_weight += weight;
    }

    if total_weight > 0.0 {
        Some(weighted_sum / total_weight)
    } else {
        None
    }
}

fn spawn_heartbeat_task(persistence: PersistenceHandle) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let event = HeartbeatEvent {
                service_time_ms: now_ms(),
                status: "ok".to_string(),
                notes: "service heartbeat".to_string(),
            };
            if let Err(err) = persistence.send_heartbeat(event) {
                error!(error = %err, "failed to persist heartbeat");
            }
        }
    });
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}
