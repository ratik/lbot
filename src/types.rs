use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    Binance,
    Bybit,
    Okx,
    Dydx,
}

impl Venue {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
            Self::Okx => "okx",
            Self::Dydx => "dydx",
        }
    }

    pub fn is_major(self) -> bool {
        !matches!(self, Self::Dydx)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Long,
    Short,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeTick {
    pub symbol: String,
    pub venue: Venue,
    pub price: f64,
    pub quantity: f64,
    pub is_buyer_maker: bool,
    pub event_time_ms: i64,
    pub recv_time_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteTick {
    pub symbol: String,
    pub venue: Venue,
    pub bid_price: f64,
    pub bid_size: f64,
    pub ask_price: f64,
    pub ask_size: f64,
    pub event_time_ms: i64,
    pub recv_time_ms: i64,
}

impl QuoteTick {
    pub fn mid_price(&self) -> f64 {
        (self.bid_price + self.ask_price) * 0.5
    }

    pub fn spread_bps(&self) -> f64 {
        let mid = self.mid_price();
        if mid <= 0.0 {
            0.0
        } else {
            ((self.ask_price - self.bid_price) / mid) * 10_000.0
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationTick {
    pub symbol: String,
    pub venue: Venue,
    pub recv_time_ms: i64,
    pub event_time_ms: i64,
    pub side: Direction,
    pub price: f64,
    pub quantity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MarketEvent {
    Trade(TradeTick),
    Quote(QuoteTick),
    Liquidation(LiquidationTick),
}

impl MarketEvent {
    pub fn symbol(&self) -> &str {
        match self {
            Self::Trade(tick) => &tick.symbol,
            Self::Quote(tick) => &tick.symbol,
            Self::Liquidation(tick) => &tick.symbol,
        }
    }

    pub fn venue(&self) -> Venue {
        match self {
            Self::Trade(tick) => tick.venue,
            Self::Quote(tick) => tick.venue,
            Self::Liquidation(tick) => tick.venue,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueScoreSnapshot {
    pub symbol: String,
    pub venue: Venue,
    pub event_time_ms: i64,
    pub price: f64,
    pub spread_bps: f64,
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
pub struct VenueSignalEvent {
    pub event_id: String,
    pub event_time_ms: i64,
    pub symbol: String,
    pub venue: Venue,
    pub direction: Direction,
    pub score: f64,
    pub price: f64,
    pub spread_bps: f64,
    pub feature_json: String,
    pub cooldown_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSignalEvent {
    pub event_id: String,
    pub event_time_ms: i64,
    pub symbol: String,
    pub direction: Direction,
    pub combined_score: f64,
    pub confirming_venues_json: String,
    pub first_triggered_venue: String,
    pub venue_spread_ms: i64,
    pub feature_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutcomeCheckEvent {
    pub event_id: String,
    pub horizon_s: u64,
    pub return_bps: f64,
    pub max_favorable_bps: f64,
    pub max_adverse_bps: f64,
    pub realized_vol_after: f64,
    pub success_flag: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationOutcomeCheckEvent {
    pub event_id: String,
    pub horizon_s: u64,
    pub expected_liq_side: Direction,
    pub price_success_flag: bool,
    pub liq_success_flag: bool,
    pub liq_event_count: u64,
    pub liq_qty_sum: f64,
    pub liq_max_qty: f64,
    pub liq_first_seen_time_ms: Option<i64>,
    pub time_to_first_liq_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatEvent {
    pub service_time_ms: i64,
    pub status: String,
    pub notes: String,
}
