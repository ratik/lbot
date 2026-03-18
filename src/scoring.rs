use serde::{Deserialize, Serialize};

use crate::{
    config::ScoreWeightsConfig,
    features::FeatureSnapshot,
    types::{Venue, VenueScoreSnapshot},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueRiskScores {
    pub directional_core_long: f64,
    pub directional_core_short: f64,
    pub stress_long: f64,
    pub stress_short: f64,
    pub min_directional_core_pass_long: bool,
    pub min_directional_core_pass_short: bool,
    pub long_score: f64,
    pub short_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketConfirmation {
    pub symbol: String,
    pub long_score: f64,
    pub short_score: f64,
    pub confirming_long: Vec<Venue>,
    pub confirming_short: Vec<Venue>,
    pub first_triggered_long: Option<Venue>,
    pub latest_triggered_long: Option<Venue>,
    pub first_triggered_short: Option<Venue>,
    pub latest_triggered_short: Option<Venue>,
    pub long_spread_ms: i64,
    pub short_spread_ms: i64,
}

pub fn compute_venue_scores(
    features: &FeatureSnapshot,
    weights: &ScoreWeightsConfig,
    min_directional_core: f64,
    max_stress_amplifier: f64,
) -> VenueRiskScores {
    let trade_burst_long = features
        .trade_count_1s_z
        .max(features.trade_count_5s_z)
        .max((-features.signed_volume_1s_z).max(0.0))
        .max((-features.signed_volume_5s_z).max(0.0))
        .max(0.0);
    let trade_burst_short = features
        .trade_count_1s_z
        .max(features.trade_count_5s_z)
        .max(features.signed_volume_1s_z.max(0.0))
        .max(features.signed_volume_5s_z.max(0.0))
        .max(0.0);
    let spread_widening_z = features.spread_bps_zscore.max(0.0).min(5.0);
    let volatility_burst_z = features
        .realized_vol_5s_z
        .max(features.realized_vol_15s_z)
        .max(0.0);

    let directional_core_long = weights.return_1s * (-features.return_1s_zscore).max(0.0)
        + weights.return_5s * (-features.return_5s_zscore).max(0.0)
        + weights.signed_volume_1s * (-features.signed_volume_1s_z).max(0.0)
        + weights.signed_volume_5s * (-features.signed_volume_5s_z).max(0.0);

    let directional_core_short = weights.return_1s * features.return_1s_zscore.max(0.0)
        + weights.return_5s * features.return_5s_zscore.max(0.0)
        + weights.signed_volume_1s * features.signed_volume_1s_z.max(0.0)
        + weights.signed_volume_5s * features.signed_volume_5s_z.max(0.0);

    let stress_long = (1.0
        + weights.trade_burst * trade_burst_long
        + weights.spread_widening * spread_widening_z
        + weights.volatility_burst * volatility_burst_z)
        .min(max_stress_amplifier);
    let stress_short = (1.0
        + weights.trade_burst * trade_burst_short
        + weights.spread_widening * spread_widening_z
        + weights.volatility_burst * volatility_burst_z)
        .min(max_stress_amplifier);

    let min_directional_core_pass_long = directional_core_long >= min_directional_core;
    let min_directional_core_pass_short = directional_core_short >= min_directional_core;
    let long_score = if min_directional_core_pass_long {
        directional_core_long * stress_long
    } else {
        0.0
    };
    let short_score = if min_directional_core_pass_short {
        directional_core_short * stress_short
    } else {
        0.0
    };

    VenueRiskScores {
        directional_core_long,
        directional_core_short,
        stress_long,
        stress_short,
        min_directional_core_pass_long,
        min_directional_core_pass_short,
        long_score,
        short_score,
    }
}

pub fn compute_market_confirmation(
    symbol: &str,
    snapshots: &[VenueScoreSnapshot],
    weights: &ScoreWeightsConfig,
    venue_threshold_long: f64,
    venue_threshold_short: f64,
) -> MarketConfirmation {
    let mut long_score = 0.0;
    let mut short_score = 0.0;
    let mut confirming_long = Vec::new();
    let mut confirming_short = Vec::new();
    let mut long_times = Vec::new();
    let mut short_times = Vec::new();

    for snapshot in snapshots
        .iter()
        .filter(|snapshot| snapshot.symbol == symbol)
    {
        let weight = weights
            .market_venue_weights
            .get(snapshot.venue.as_str())
            .copied()
            .unwrap_or(1.0);
        long_score += snapshot.long_score * weight;
        short_score += snapshot.short_score * weight;
        if snapshot.min_directional_core_pass_long && snapshot.long_score >= venue_threshold_long {
            confirming_long.push(snapshot.venue);
            long_times.push((snapshot.event_time_ms, snapshot.venue));
        }
        if snapshot.min_directional_core_pass_short && snapshot.short_score >= venue_threshold_short
        {
            confirming_short.push(snapshot.venue);
            short_times.push((snapshot.event_time_ms, snapshot.venue));
        }
    }

    long_times.sort_by_key(|entry| entry.0);
    short_times.sort_by_key(|entry| entry.0);

    let long_spread_ms = match (long_times.first(), long_times.last()) {
        (Some(first), Some(last)) => last.0 - first.0,
        _ => 0,
    };
    let short_spread_ms = match (short_times.first(), short_times.last()) {
        (Some(first), Some(last)) => last.0 - first.0,
        _ => 0,
    };

    MarketConfirmation {
        symbol: symbol.to_string(),
        long_score,
        short_score,
        confirming_long,
        confirming_short,
        first_triggered_long: long_times.first().map(|entry| entry.1),
        latest_triggered_long: long_times.last().map(|entry| entry.1),
        first_triggered_short: short_times.first().map(|entry| entry.1),
        latest_triggered_short: short_times.last().map(|entry| entry.1),
        long_spread_ms,
        short_spread_ms,
    }
}
