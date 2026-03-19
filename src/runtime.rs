use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
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
    long_gate_blocked: u64,
    short_gate_blocked: u64,
    failed_by_gate: u64,
    failed_by_threshold: u64,
    dual_pass_stronger_only: u64,
}

impl BlockCounters {
    fn note(&mut self, kind: BlockKind, symbol: &str, venue: Venue, detail: &str) {
        let counter = match kind {
            BlockKind::Warmup => &mut self.warmup,
            BlockKind::LongGateBlocked => &mut self.long_gate_blocked,
            BlockKind::ShortGateBlocked => &mut self.short_gate_blocked,
            BlockKind::FailedByGate => &mut self.failed_by_gate,
            BlockKind::FailedByThreshold => &mut self.failed_by_threshold,
            BlockKind::DualPassStrongerOnly => &mut self.dual_pass_stronger_only,
        };
        *counter += 1;
        if *counter <= 3 || *counter % 10_000 == 0 {
            debug!(
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
    LongGateBlocked,
    ShortGateBlocked,
    FailedByGate,
    FailedByThreshold,
    DualPassStrongerOnly,
}

impl BlockKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Warmup => "warmup",
            Self::LongGateBlocked => "long_gate_blocked",
            Self::ShortGateBlocked => "short_gate_blocked",
            Self::FailedByGate => "failed_by_gate",
            Self::FailedByThreshold => "failed_by_threshold",
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

#[derive(Debug, Default)]
struct RuntimeStats {
    evaluations: u64,
    venue_signals: u64,
    market_signals: u64,
    liquidation_events_seen: u64,
    venue_signal_counts: HashMap<(String, Venue), u64>,
    interval_venue_signal_counts: HashMap<(String, Venue), u64>,
}

impl RuntimeStats {
    fn note_evaluation(&mut self) {
        self.evaluations += 1;
    }

    fn note_venue_signal(&mut self, symbol: &str, venue: Venue) {
        self.venue_signals += 1;
        *self
            .venue_signal_counts
            .entry((symbol.to_string(), venue))
            .or_insert(0) += 1;
        *self
            .interval_venue_signal_counts
            .entry((symbol.to_string(), venue))
            .or_insert(0) += 1;
    }

    fn note_market_signal(&mut self) {
        self.market_signals += 1;
    }

    fn note_liquidation_event(&mut self) {
        self.liquidation_events_seen += 1;
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct SummarySnapshot {
    evaluations: u64,
    venue_signals: u64,
    market_signals: u64,
    liquidation_events_seen: u64,
    blocked_warmup: u64,
    blocked_long_gate: u64,
    blocked_short_gate: u64,
    failed_by_gate: u64,
    failed_by_threshold: u64,
    blocked_dual_pass: u64,
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
    let mut runtime_stats = RuntimeStats::default();
    let mut last_summary_ms = now_ms();
    let mut last_summary_snapshot = SummarySnapshot::default();
    let market_path_enabled =
        config.enabled_venues().len() >= config.thresholds.market_min_confirmations;

    info!(
        symbols = ?config.symbols,
        venues = ?config.enabled_venues().iter().map(|venue| venue.as_str()).collect::<Vec<_>>(),
        sqlite_path = %config.sqlite_path,
        market_path_enabled,
        "startup complete"
    );

    while let Some(event) = rx.recv().await {
        if let MarketEvent::Liquidation(tick) = event {
            if let Err(err) = persistence.send_liquidation_event(tick.clone()) {
                error!(
                    error = %err,
                    symbol = %tick.symbol,
                    venue = tick.venue.as_str(),
                    "failed to persist liquidation event"
                );
            } else {
                runtime_stats.note_liquidation_event();
            }
            continue;
        }

        let key = (event.symbol().to_string(), event.venue());
        let symbol = key.0.clone();
        let venue = key.1;

        if !config.venue_analysis_enabled(venue) {
            continue;
        }

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
                MarketEvent::Liquidation(_) => unreachable!("liquidation events continue early"),
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
            runtime_stats.note_evaluation();
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
            let snapshot_time_ms = if features.bucket_time_ms > 0 {
                features.bucket_time_ms
            } else {
                now_ms()
            };
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
                &mut runtime_stats,
                &snapshot,
                &features,
                warmup,
            );

            if market_path_enabled {
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
                    &mut runtime_stats,
                    &symbol_snapshots,
                    &market,
                );
            }
        }

        let summary_now_ms = now_ms();
        if summary_now_ms - last_summary_ms >= 60_000 {
            log_runtime_summary(
                &mut runtime_stats,
                &block_counters,
                &venue_states,
                &config,
                summary_now_ms,
                &mut last_summary_snapshot,
            );
            last_summary_ms = summary_now_ms;
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
    runtime_stats: &mut RuntimeStats,
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
    let spread_shock = features.spread_bps_zscore >= 5.0;

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

    if !long_directional_gate {
        block_counters.note(
            BlockKind::LongGateBlocked,
            &snapshot.symbol,
            snapshot.venue,
            "long directional gate blocked",
        );
    }
    if !short_directional_gate {
        block_counters.note(
            BlockKind::ShortGateBlocked,
            &snapshot.symbol,
            snapshot.venue,
            "short directional gate blocked",
        );
    }

    let selected = match (long_candidate, short_candidate) {
        (false, false) => {
            if !long_directional_gate && !short_directional_gate {
                block_counters.note(
                    BlockKind::FailedByGate,
                    &snapshot.symbol,
                    snapshot.venue,
                    "no candidates survived directional gate",
                );
            } else {
                block_counters.note(
                    BlockKind::FailedByThreshold,
                    &snapshot.symbol,
                    snapshot.venue,
                    "directional gate passed on at least one side but threshold failed",
                );
            }
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
    let emitted_directional_core = match direction {
        Direction::Long => snapshot.directional_core_long,
        Direction::Short => snapshot.directional_core_short,
    };
    let strong_directional_core = emitted_directional_core >= config.min_directional_core;
    let volume_margin = 0.25;
    let volume_confirmed = match direction {
        Direction::Long => {
            features.signed_volume_1s_z
                <= -(config.directional_gates.long_volume_z_gate + volume_margin)
        }
        Direction::Short => {
            features.signed_volume_1s_z
                >= config.directional_gates.short_volume_z_gate + volume_margin
        }
    };
    let component_contributions = component_contributions(config, features, direction);
    let reason_tags = reason_tags(
        config,
        features,
        direction,
        emitted_directional_core,
        volume_confirmed,
        &component_contributions,
    );
    let feature_json = match serde_json::to_string(&serde_json::json!({
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
        "diagnostics": {
            "spread_shock": spread_shock,
            "strong_directional_core": strong_directional_core,
            "volume_confirmed": volume_confirmed,
            "emitted_direction": direction.as_str(),
        },
        "reason_tags": reason_tags.clone(),
        "component_contributions": component_contributions,
        "features": features,
    })) {
        Ok(json) => json,
        Err(err) => {
            warn!(error = %err, "failed serializing venue features");
            return;
        }
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
        runtime_stats.note_venue_signal(&event.symbol, event.venue);

        warn!(
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
            spread_shock,
            strong_directional_core,
            volume_confirmed,
            reason_tags = ?reason_tags,
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
    runtime_stats: &mut RuntimeStats,
    symbol_snapshots: &[VenueScoreSnapshot],
    market: &crate::scoring::MarketConfirmation,
) {
    let feature_json = match serde_json::to_string(&serde_json::json!({
        "market": market,
        "note": "market payload excludes venue-local feature snapshot",
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
            runtime_stats.note_market_signal();

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

fn component_contributions(
    config: &AppConfig,
    features: &FeatureSnapshot,
    direction: Direction,
) -> serde_json::Value {
    let return_1s = match direction {
        Direction::Long => config.score_weights.return_1s * (-features.return_1s_zscore).max(0.0),
        Direction::Short => config.score_weights.return_1s * features.return_1s_zscore.max(0.0),
    };
    let return_5s = match direction {
        Direction::Long => config.score_weights.return_5s * (-features.return_5s_zscore).max(0.0),
        Direction::Short => config.score_weights.return_5s * features.return_5s_zscore.max(0.0),
    };
    let signed_volume_1s = match direction {
        Direction::Long => {
            config.score_weights.signed_volume_1s * (-features.signed_volume_1s_z).max(0.0)
        }
        Direction::Short => {
            config.score_weights.signed_volume_1s * features.signed_volume_1s_z.max(0.0)
        }
    };
    let signed_volume_5s = match direction {
        Direction::Long => {
            config.score_weights.signed_volume_5s * (-features.signed_volume_5s_z).max(0.0)
        }
        Direction::Short => {
            config.score_weights.signed_volume_5s * features.signed_volume_5s_z.max(0.0)
        }
    };
    let directional_trade_burst = match direction {
        Direction::Long => features
            .trade_count_1s_z
            .max(features.trade_count_5s_z)
            .max((-features.signed_volume_1s_z).max(0.0))
            .max((-features.signed_volume_5s_z).max(0.0))
            .max(0.0),
        Direction::Short => features
            .trade_count_1s_z
            .max(features.trade_count_5s_z)
            .max(features.signed_volume_1s_z.max(0.0))
            .max(features.signed_volume_5s_z.max(0.0))
            .max(0.0),
    };
    let capped_spread_z = features.spread_bps_zscore.max(0.0).min(5.0);
    let volatility_burst_z = features
        .realized_vol_5s_z
        .max(features.realized_vol_15s_z)
        .max(0.0);
    let trade_burst = config.score_weights.trade_burst * directional_trade_burst;
    let spread_widening = config.score_weights.spread_widening * capped_spread_z;
    let volatility_burst = config.score_weights.volatility_burst * volatility_burst_z;
    let directional_core = return_1s + return_5s + signed_volume_1s + signed_volume_5s;
    let stress =
        (1.0 + trade_burst + spread_widening + volatility_burst).min(config.max_stress_amplifier);

    serde_json::json!({
        "direction": direction.as_str(),
        "return_1s": return_1s,
        "return_5s": return_5s,
        "signed_volume_1s": signed_volume_1s,
        "signed_volume_5s": signed_volume_5s,
        "trade_burst": trade_burst,
        "spread_widening": spread_widening,
        "volatility_burst": volatility_burst,
        "directional_core": directional_core,
        "stress": stress,
    })
}

fn reason_tags(
    config: &AppConfig,
    features: &FeatureSnapshot,
    direction: Direction,
    emitted_directional_core: f64,
    volume_confirmed: bool,
    component_contributions: &serde_json::Value,
) -> Vec<String> {
    let mut tags = Vec::new();
    match direction {
        Direction::Long => {
            if features.return_1s_zscore <= -config.directional_gates.long_return_z_gate {
                tags.push("downside_return_impulse".to_string());
            }
            if features.signed_volume_1s_z <= -config.directional_gates.long_volume_z_gate {
                tags.push("sell_flow_impulse".to_string());
            }
        }
        Direction::Short => {
            if features.return_1s_zscore >= config.directional_gates.short_return_z_gate {
                tags.push("upside_return_impulse".to_string());
            }
            if features.signed_volume_1s_z >= config.directional_gates.short_volume_z_gate {
                tags.push("buy_flow_impulse".to_string());
            }
        }
    }

    let trade_burst = component_contributions["trade_burst"]
        .as_f64()
        .unwrap_or_default();
    let spread_widening = component_contributions["spread_widening"]
        .as_f64()
        .unwrap_or_default();
    let volatility_burst = component_contributions["volatility_burst"]
        .as_f64()
        .unwrap_or_default();
    if trade_burst >= 0.20 {
        tags.push("trade_burst".to_string());
    }
    if spread_widening > 0.0 {
        tags.push("spread_widening".to_string());
    }
    if features.spread_bps_zscore >= 5.0 {
        tags.push("spread_shock".to_string());
    }
    if volatility_burst > 0.0 {
        tags.push("volatility_burst".to_string());
    }
    if emitted_directional_core >= config.min_directional_core {
        tags.push("strong_directional_core".to_string());
    }
    if volume_confirmed {
        tags.push("volume_confirmed".to_string());
    }
    tags
}

fn log_runtime_summary(
    runtime_stats: &mut RuntimeStats,
    block_counters: &BlockCounters,
    venue_states: &HashMap<(String, Venue), RollingState>,
    config: &AppConfig,
    now_ms: i64,
    last_summary_snapshot: &mut SummarySnapshot,
) {
    let active_states = venue_states.len();
    let eligible_states = venue_states
        .values()
        .filter(|state| warmup_status(config, state, now_ms).eligible)
        .count();
    let per_state = if runtime_stats.venue_signal_counts.is_empty() {
        "none".to_string()
    } else {
        runtime_stats
            .venue_signal_counts
            .iter()
            .map(|((symbol, venue), count)| format!("{symbol}:{}={count}", venue.as_str()))
            .collect::<Vec<_>>()
            .join(",")
    };
    let current_snapshot = SummarySnapshot {
        evaluations: runtime_stats.evaluations,
        venue_signals: runtime_stats.venue_signals,
        market_signals: runtime_stats.market_signals,
        liquidation_events_seen: runtime_stats.liquidation_events_seen,
        blocked_warmup: block_counters.warmup,
        blocked_long_gate: block_counters.long_gate_blocked,
        blocked_short_gate: block_counters.short_gate_blocked,
        failed_by_gate: block_counters.failed_by_gate,
        failed_by_threshold: block_counters.failed_by_threshold,
        blocked_dual_pass: block_counters.dual_pass_stronger_only,
    };
    let evaluations_interval = current_snapshot.evaluations - last_summary_snapshot.evaluations;
    let venue_signals_interval =
        current_snapshot.venue_signals - last_summary_snapshot.venue_signals;
    let market_signals_interval =
        current_snapshot.market_signals - last_summary_snapshot.market_signals;
    let liquidation_events_interval =
        current_snapshot.liquidation_events_seen - last_summary_snapshot.liquidation_events_seen;
    let blocked_warmup_interval =
        current_snapshot.blocked_warmup - last_summary_snapshot.blocked_warmup;
    let blocked_long_gate_interval =
        current_snapshot.blocked_long_gate - last_summary_snapshot.blocked_long_gate;
    let blocked_short_gate_interval =
        current_snapshot.blocked_short_gate - last_summary_snapshot.blocked_short_gate;
    let failed_by_gate_interval =
        current_snapshot.failed_by_gate - last_summary_snapshot.failed_by_gate;
    let failed_by_threshold_interval =
        current_snapshot.failed_by_threshold - last_summary_snapshot.failed_by_threshold;
    let blocked_dual_pass_interval =
        current_snapshot.blocked_dual_pass - last_summary_snapshot.blocked_dual_pass;
    let per_state_interval = if runtime_stats.interval_venue_signal_counts.is_empty() {
        "none".to_string()
    } else {
        runtime_stats
            .interval_venue_signal_counts
            .iter()
            .map(|((symbol, venue), count)| format!("{symbol}:{}={count}", venue.as_str()))
            .collect::<Vec<_>>()
            .join(",")
    };
    *last_summary_snapshot = current_snapshot;

    info!(
        evaluations = runtime_stats.evaluations,
        evaluations_interval,
        venue_signals = runtime_stats.venue_signals,
        venue_signals_interval,
        market_signals = runtime_stats.market_signals,
        market_signals_interval,
        liquidation_events_seen = runtime_stats.liquidation_events_seen,
        liquidation_events_interval,
        blocked_warmup = block_counters.warmup,
        blocked_warmup_interval,
        blocked_long_gate = block_counters.long_gate_blocked,
        blocked_long_gate_interval,
        blocked_short_gate = block_counters.short_gate_blocked,
        blocked_short_gate_interval,
        failed_by_gate = block_counters.failed_by_gate,
        failed_by_gate_interval,
        failed_by_threshold = block_counters.failed_by_threshold,
        failed_by_threshold_interval,
        blocked_dual_pass = block_counters.dual_pass_stronger_only,
        blocked_dual_pass_interval,
        active_states,
        eligible_states,
        venue_long_threshold = config.thresholds.venue_long_threshold,
        venue_short_threshold = config.thresholds.venue_short_threshold,
        min_directional_core = config.min_directional_core,
        long_return_z_gate = config.directional_gates.long_return_z_gate,
        long_volume_z_gate = config.directional_gates.long_volume_z_gate,
        short_return_z_gate = config.directional_gates.short_return_z_gate,
        short_volume_z_gate = config.directional_gates.short_volume_z_gate,
        spread_widening_weight = config.score_weights.spread_widening,
        venue_event_s = config.cooldowns.venue_event_s,
        per_state_total = %per_state,
        per_state_interval = %per_state_interval,
        "runtime summary"
    );

    runtime_stats.interval_venue_signal_counts.clear();
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
