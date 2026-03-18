use std::collections::VecDeque;

use crate::config::FeatureWindowsConfig;
use crate::types::{QuoteTick, TradeTick, Venue};

#[derive(Debug, Clone)]
pub struct TimeBucket {
    pub bucket_key: i64,
    pub bucket_start_ms: i64,
    pub signed_volume: f64,
    pub signed_trade_count: i64,
    pub trade_count: u32,
    pub spread_bps_last: Option<f64>,
    pub mid_last: Option<f64>,
    pub sq_return_sum: f64,
    pub return_obs: u32,
}

impl TimeBucket {
    fn new(bucket_key: i64, bucket_start_ms: i64) -> Self {
        Self {
            bucket_key,
            bucket_start_ms,
            signed_volume: 0.0,
            signed_trade_count: 0,
            trade_count: 0,
            spread_bps_last: None,
            mid_last: None,
            sq_return_sum: 0.0,
            return_obs: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RollingState {
    retention_ms: i64,
    bucket_ms: i64,
    first_seen_ms: Option<i64>,
    trades: VecDeque<TradeTick>,
    quotes: VecDeque<QuoteTick>,
    time_buckets: VecDeque<TimeBucket>,
    last_mid: Option<f64>,
    latest_quote: Option<QuoteTick>,
}

impl RollingState {
    pub fn new(
        symbol: String,
        venue: Venue,
        windows: &FeatureWindowsConfig,
        online_bucket_ms: u64,
    ) -> Self {
        let _ = (symbol, venue);
        Self {
            retention_ms: (windows.state_retention_s as i64) * 1000,
            bucket_ms: online_bucket_ms as i64,
            first_seen_ms: None,
            trades: VecDeque::new(),
            quotes: VecDeque::new(),
            time_buckets: VecDeque::new(),
            last_mid: None,
            latest_quote: None,
        }
    }

    pub fn apply_trade(&mut self, trade: TradeTick) {
        self.first_seen_ms.get_or_insert(trade.recv_time_ms);
        let bucket = self.bucket_mut(trade.recv_time_ms);
        let signed_qty = if trade.is_buyer_maker {
            -trade.quantity
        } else {
            trade.quantity
        };
        bucket.trade_count += 1;
        bucket.signed_trade_count += if trade.is_buyer_maker { -1 } else { 1 };
        bucket.signed_volume += signed_qty;
        self.trades.push_back(trade.clone());
        self.prune(trade.recv_time_ms);
    }

    pub fn apply_quote(&mut self, quote: QuoteTick) {
        self.first_seen_ms.get_or_insert(quote.recv_time_ms);
        let spread_bps = quote.spread_bps();
        let mid = quote.mid_price();
        let previous_mid = self.last_mid;
        let bucket = self.bucket_mut(quote.recv_time_ms);
        bucket.spread_bps_last = Some(spread_bps);
        bucket.mid_last = Some(mid);
        if let Some(last_mid) = previous_mid {
            let ret = (mid / last_mid) - 1.0;
            bucket.sq_return_sum += ret * ret;
            bucket.return_obs += 1;
        }
        self.last_mid = Some(mid);
        self.latest_quote = Some(quote.clone());
        self.quotes.push_back(quote.clone());
        self.prune(quote.recv_time_ms);
    }

    pub fn latest_mid(&self) -> Option<f64> {
        self.latest_quote.as_ref().map(QuoteTick::mid_price)
    }

    pub fn latest_spread_bps(&self) -> Option<f64> {
        self.latest_quote.as_ref().map(QuoteTick::spread_bps)
    }

    pub fn time_buckets(&self) -> &VecDeque<TimeBucket> {
        &self.time_buckets
    }

    pub fn first_seen_ms(&self) -> Option<i64> {
        self.first_seen_ms
    }

    pub fn bucket_ms(&self) -> i64 {
        self.bucket_ms
    }

    pub fn has_min_buckets_for(&self, duration_ms: i64) -> bool {
        let required = ((duration_ms + self.bucket_ms - 1) / self.bucket_ms).max(1) as usize;
        self.time_buckets.len() >= required
    }

    fn bucket_mut(&mut self, recv_time_ms: i64) -> &mut TimeBucket {
        let bucket_key = recv_time_ms / self.bucket_ms;
        let bucket_start_ms = bucket_key * self.bucket_ms;
        if self
            .time_buckets
            .back()
            .map(|bucket| bucket.bucket_key != bucket_key)
            .unwrap_or(true)
        {
            self.time_buckets
                .push_back(TimeBucket::new(bucket_key, bucket_start_ms));
        }
        self.time_buckets
            .back_mut()
            .expect("time bucket should exist")
    }

    fn prune(&mut self, now_ms: i64) {
        let cutoff_ms = now_ms - self.retention_ms;
        while self
            .trades
            .front()
            .map(|tick| tick.recv_time_ms < cutoff_ms)
            .unwrap_or(false)
        {
            self.trades.pop_front();
        }
        while self
            .quotes
            .front()
            .map(|tick| tick.recv_time_ms < cutoff_ms)
            .unwrap_or(false)
        {
            self.quotes.pop_front();
        }
        let cutoff_bucket = cutoff_ms / self.bucket_ms;
        while self
            .time_buckets
            .front()
            .map(|bucket| bucket.bucket_key < cutoff_bucket)
            .unwrap_or(false)
        {
            self.time_buckets.pop_front();
        }
    }
}
