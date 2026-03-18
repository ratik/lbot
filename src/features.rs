use serde::{Deserialize, Serialize};

use crate::{
    config::{FeatureWindowsConfig, ZscoreWindowsConfig},
    state::{RollingState, TimeBucket},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureSnapshot {
    pub bucket_time_ms: i64,
    pub return_1s: f64,
    pub return_5s: f64,
    pub return_15s: f64,
    pub signed_trade_count_1s: f64,
    pub signed_trade_count_5s: f64,
    pub signed_volume_1s: f64,
    pub signed_volume_5s: f64,
    pub trades_per_sec_1s: f64,
    pub trades_per_sec_5s: f64,
    pub signed_volume_1s_z: f64,
    pub signed_volume_5s_z: f64,
    pub trade_count_1s_z: f64,
    pub trade_count_5s_z: f64,
    pub spread_bps_now: f64,
    pub spread_bps_zscore: f64,
    pub realized_vol_5s: f64,
    pub realized_vol_15s: f64,
    pub realized_vol_5s_z: f64,
    pub realized_vol_15s_z: f64,
    pub return_1s_zscore: f64,
    pub return_5s_zscore: f64,
}

pub fn compute_features(
    state: &RollingState,
    windows: &FeatureWindowsConfig,
    zscores: &ZscoreWindowsConfig,
) -> Option<FeatureSnapshot> {
    let latest_mid = state.latest_mid()?;
    let latest_spread_bps = state.latest_spread_bps().unwrap_or_default();
    let buckets = state.time_buckets();
    let now_bucket_time_ms = buckets.back()?.bucket_start_ms;
    let bucket_ms = state.bucket_ms();

    let return_1s = lookup_return(
        buckets,
        latest_mid,
        now_bucket_time_ms - (windows.return_1s as i64 * 1000),
    );
    let return_5s = lookup_return(
        buckets,
        latest_mid,
        now_bucket_time_ms - (windows.return_5s as i64 * 1000),
    );
    let return_15s = lookup_return(
        buckets,
        latest_mid,
        now_bucket_time_ms - (windows.return_15s as i64 * 1000),
    );

    let one_sec = trailing_buckets_ms(buckets, 1_000);
    let five_sec = trailing_buckets_ms(buckets, 5_000);
    let fifteen_sec = trailing_buckets_ms(buckets, 15_000);

    let signed_trade_count_1s = one_sec
        .iter()
        .map(|bucket| bucket.signed_trade_count as f64)
        .sum();
    let signed_trade_count_5s = five_sec
        .iter()
        .map(|bucket| bucket.signed_trade_count as f64)
        .sum();
    let signed_volume_1s = one_sec.iter().map(|bucket| bucket.signed_volume).sum();
    let signed_volume_5s = five_sec.iter().map(|bucket| bucket.signed_volume).sum();
    let trades_1s: f64 = one_sec.iter().map(|bucket| bucket.trade_count as f64).sum();
    let trades_5s: f64 = five_sec
        .iter()
        .map(|bucket| bucket.trade_count as f64)
        .sum();
    let trades_per_sec_1s = if one_sec.is_empty() {
        0.0
    } else {
        trades_1s / (one_sec.len() as f64 * bucket_ms as f64 / 1000.0)
    };
    let trades_per_sec_5s = if five_sec.is_empty() {
        0.0
    } else {
        trades_5s / (five_sec.len() as f64 * bucket_ms as f64 / 1000.0)
    };

    let realized_vol_5s = realized_vol(&five_sec);
    let realized_vol_15s = realized_vol(&fifteen_sec);

    let zscore_window_ms = zscores.seconds as i64 * 1000;
    let signed_volume_1s_series = aggregate_series(buckets, 1_000, zscore_window_ms);
    let signed_volume_5s_series = aggregate_series(buckets, 5_000, zscore_window_ms);
    let trade_count_1s_series = aggregate_series_by(buckets, 1_000, zscore_window_ms, |bucket| {
        bucket.trade_count as f64
    });
    let trade_count_5s_series = aggregate_series_by(buckets, 5_000, zscore_window_ms, |bucket| {
        bucket.trade_count as f64
    });
    let spread_series: Vec<f64> = buckets
        .iter()
        .rev()
        .take(required_bucket_count(zscore_window_ms, bucket_ms))
        .filter_map(|bucket| bucket.spread_bps_last)
        .collect();
    let returns_1s_series = returns_series(buckets, 1_000, zscore_window_ms);
    let returns_5s_series = returns_series(buckets, 5_000, zscore_window_ms);
    let vol_5s_series = realized_vol_series(buckets, 5_000, zscore_window_ms);
    let vol_15s_series = realized_vol_series(buckets, 15_000, zscore_window_ms);

    Some(FeatureSnapshot {
        bucket_time_ms: now_bucket_time_ms,
        return_1s,
        return_5s,
        return_15s,
        signed_trade_count_1s,
        signed_trade_count_5s,
        signed_volume_1s,
        signed_volume_5s,
        trades_per_sec_1s,
        trades_per_sec_5s,
        signed_volume_1s_z: zscore(signed_volume_1s, &signed_volume_1s_series),
        signed_volume_5s_z: zscore(signed_volume_5s, &signed_volume_5s_series),
        trade_count_1s_z: zscore(trades_1s, &trade_count_1s_series),
        trade_count_5s_z: zscore(trades_5s, &trade_count_5s_series),
        spread_bps_now: latest_spread_bps,
        spread_bps_zscore: zscore(latest_spread_bps, &spread_series),
        realized_vol_5s,
        realized_vol_15s,
        realized_vol_5s_z: zscore(realized_vol_5s, &vol_5s_series),
        realized_vol_15s_z: zscore(realized_vol_15s, &vol_15s_series),
        return_1s_zscore: zscore(return_1s, &returns_1s_series),
        return_5s_zscore: zscore(return_5s, &returns_5s_series),
    })
}

fn trailing_buckets_ms(
    buckets: &std::collections::VecDeque<TimeBucket>,
    duration_ms: i64,
) -> Vec<TimeBucket> {
    let cutoff = buckets
        .back()
        .map(|bucket| bucket.bucket_start_ms - duration_ms + 1)
        .unwrap_or_default();
    buckets
        .iter()
        .filter(|bucket| bucket.bucket_start_ms >= cutoff)
        .cloned()
        .collect()
}

fn lookup_return(
    buckets: &std::collections::VecDeque<TimeBucket>,
    latest_mid: f64,
    target_time_ms: i64,
) -> f64 {
    let maybe_base = buckets
        .iter()
        .rev()
        .find(|bucket| bucket.bucket_start_ms <= target_time_ms)
        .and_then(|bucket| bucket.mid_last);
    maybe_base
        .map(|base| (latest_mid / base) - 1.0)
        .unwrap_or(0.0)
}

fn returns_series(
    buckets: &std::collections::VecDeque<TimeBucket>,
    interval_ms: i64,
    sample_window_ms: i64,
) -> Vec<f64> {
    let mids: Vec<TimedMid> = buckets
        .iter()
        .filter_map(|bucket| {
            bucket.mid_last.map(|mid| TimedMid {
                bucket_start_ms: bucket.bucket_start_ms,
                value: mid,
            })
        })
        .collect();
    let mut series = Vec::new();
    let sample_cutoff = mids
        .last()
        .map(|mid| mid.bucket_start_ms - sample_window_ms)
        .unwrap_or_default();
    for idx in 0..mids.len() {
        let curr = &mids[idx];
        if curr.bucket_start_ms < sample_cutoff {
            continue;
        }
        if let Some(prev) = mids[..idx]
            .iter()
            .rev()
            .find(|prev| prev.bucket_start_ms <= curr.bucket_start_ms - interval_ms)
        {
            series.push((curr.value / prev.value) - 1.0);
        }
    }
    series
}

fn realized_vol_series(
    buckets: &std::collections::VecDeque<TimeBucket>,
    interval_ms: i64,
    sample_window_ms: i64,
) -> Vec<f64> {
    let buckets = buckets.iter().cloned().collect::<Vec<_>>();
    let mut series = Vec::new();
    let sample_cutoff = buckets
        .last()
        .map(|bucket| bucket.bucket_start_ms - sample_window_ms)
        .unwrap_or_default();
    for idx in 0..buckets.len() {
        if buckets[idx].bucket_start_ms < sample_cutoff {
            continue;
        }
        let start_time = buckets[idx].bucket_start_ms - interval_ms + 1;
        let start = buckets
            .iter()
            .position(|bucket| bucket.bucket_start_ms >= start_time)
            .unwrap_or(0);
        let window = &buckets[start..=idx];
        let sq_sum: f64 = window.iter().map(|bucket| bucket.sq_return_sum).sum();
        series.push(sq_sum.sqrt());
    }
    series
}

fn realized_vol(buckets: &[TimeBucket]) -> f64 {
    buckets
        .iter()
        .map(|bucket| bucket.sq_return_sum)
        .sum::<f64>()
        .sqrt()
}

fn aggregate_series(
    buckets: &std::collections::VecDeque<TimeBucket>,
    aggregate_ms: i64,
    sample_window_ms: i64,
) -> Vec<f64> {
    aggregate_series_by(buckets, aggregate_ms, sample_window_ms, |bucket| {
        bucket.signed_volume
    })
}

fn aggregate_series_by<F>(
    buckets: &std::collections::VecDeque<TimeBucket>,
    aggregate_ms: i64,
    sample_window_ms: i64,
    value_fn: F,
) -> Vec<f64>
where
    F: Fn(&TimeBucket) -> f64,
{
    let buckets = buckets.iter().cloned().collect::<Vec<_>>();
    let sample_cutoff = buckets
        .last()
        .map(|bucket| bucket.bucket_start_ms - sample_window_ms)
        .unwrap_or_default();
    let mut series = Vec::new();
    for idx in 0..buckets.len() {
        if buckets[idx].bucket_start_ms < sample_cutoff {
            continue;
        }
        let start_time = buckets[idx].bucket_start_ms - aggregate_ms + 1;
        let start = buckets
            .iter()
            .position(|bucket| bucket.bucket_start_ms >= start_time)
            .unwrap_or(0);
        let aggregate = buckets[start..=idx].iter().map(&value_fn).sum();
        series.push(aggregate);
    }
    series
}

fn required_bucket_count(window_ms: i64, bucket_ms: i64) -> usize {
    ((window_ms + bucket_ms - 1) / bucket_ms).max(1) as usize
}

fn zscore(current: f64, series: &[f64]) -> f64 {
    if series.len() < 5 {
        return 0.0;
    }
    let mean = series.iter().sum::<f64>() / series.len() as f64;
    let variance = series
        .iter()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / series.len() as f64;
    let stddev = variance.sqrt();
    if stddev <= 1e-9 {
        0.0
    } else {
        (current - mean) / stddev
    }
}

#[derive(Debug, Clone)]
struct TimedMid {
    bucket_start_ms: i64,
    value: f64,
}
