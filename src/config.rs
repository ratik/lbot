use std::{collections::HashMap, env, fs, path::PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::types::Venue;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_symbols")]
    pub symbols: Vec<String>,
    #[serde(default = "default_online_bucket_ms")]
    pub online_bucket_ms: u64,
    #[serde(default = "default_warmup_seconds")]
    pub warmup_seconds: u64,
    #[serde(default = "default_venue_threshold")]
    pub venue_signal_threshold: f64,
    #[serde(default = "default_min_directional_core")]
    pub min_directional_core: f64,
    #[serde(default = "default_max_stress_amplifier")]
    pub max_stress_amplifier: f64,
    #[serde(default = "default_min_return_bps")]
    pub min_return_bps: f64,
    #[serde(default, flatten)]
    pub directional_gates: DirectionalGateConfig,
    #[serde(default)]
    pub venues: VenueConfig,
    #[serde(default)]
    pub feature_windows: FeatureWindowsConfig,
    #[serde(default)]
    pub zscore_windows: ZscoreWindowsConfig,
    #[serde(default)]
    pub score_weights: ScoreWeightsConfig,
    #[serde(default)]
    pub thresholds: ThresholdsConfig,
    #[serde(default)]
    pub cooldowns: CooldownConfig,
    #[serde(default)]
    pub evaluation: EvaluationConfig,
    #[serde(default)]
    pub sqlite_path: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let path = env::args()
            .nth(1)
            .map(PathBuf::from)
            .or_else(|| env::var("LBOT_CONFIG").ok().map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("config.toml"));

        let mut config = if path.exists() {
            let raw = fs::read_to_string(&path)
                .with_context(|| format!("failed reading config {}", path.display()))?;
            toml::from_str::<Self>(&raw)
                .with_context(|| format!("failed parsing config {}", path.display()))?
        } else {
            Self::default()
        };

        config.apply_env_overrides();
        config.normalize();
        Ok(config)
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(symbols) = env::var("LBOT_SYMBOLS") {
            self.symbols = split_csv(&symbols);
        }
        if let Ok(path) = env::var("LBOT_SQLITE_PATH") {
            self.sqlite_path = path;
        }
        if let Ok(level) = env::var("LBOT_LOG_LEVEL") {
            self.log_level = level;
        }
        if let Ok(enabled) = env::var("LBOT_ENABLED_VENUES") {
            let set = split_csv(&enabled);
            self.venues.binance.enabled = set.iter().any(|v| v == "binance");
            self.venues.bybit.enabled = set.iter().any(|v| v == "bybit");
            self.venues.okx.enabled = set.iter().any(|v| v == "okx");
            self.venues.dydx.enabled = set.iter().any(|v| v == "dydx");
        }
    }

    fn normalize(&mut self) {
        self.symbols = self
            .symbols
            .iter()
            .map(|s| s.trim().to_uppercase())
            .collect();
        self.thresholds.venue_long_threshold = self.venue_signal_threshold;
        self.thresholds.venue_short_threshold = self.venue_signal_threshold;
    }

    pub fn enabled_venues(&self) -> Vec<Venue> {
        let mut venues = Vec::new();
        if self.venues.binance.enabled {
            venues.push(Venue::Binance);
        }
        if self.venues.bybit.enabled {
            venues.push(Venue::Bybit);
        }
        if self.venues.okx.enabled {
            venues.push(Venue::Okx);
        }
        if self.venues.dydx.enabled {
            venues.push(Venue::Dydx);
        }
        venues
    }

    pub fn symbol_success_threshold_bps(&self, symbol: &str) -> f64 {
        self.evaluation
            .success_thresholds_bps
            .get(symbol)
            .copied()
            .unwrap_or(self.evaluation.default_success_threshold_bps)
    }

    pub fn venue_analysis_enabled(&self, venue: Venue) -> bool {
        match venue {
            Venue::Binance => self.venues.binance.analysis_enabled,
            Venue::Bybit => self.venues.bybit.analysis_enabled,
            Venue::Okx => self.venues.okx.analysis_enabled,
            Venue::Dydx => self.venues.dydx.analysis_enabled,
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            symbols: default_symbols(),
            online_bucket_ms: default_online_bucket_ms(),
            warmup_seconds: default_warmup_seconds(),
            venue_signal_threshold: default_venue_threshold(),
            min_directional_core: default_min_directional_core(),
            max_stress_amplifier: default_max_stress_amplifier(),
            min_return_bps: default_min_return_bps(),
            directional_gates: DirectionalGateConfig::default(),
            venues: VenueConfig::default(),
            feature_windows: FeatureWindowsConfig::default(),
            zscore_windows: ZscoreWindowsConfig::default(),
            score_weights: ScoreWeightsConfig::default(),
            thresholds: ThresholdsConfig::default(),
            cooldowns: CooldownConfig::default(),
            evaluation: EvaluationConfig::default(),
            sqlite_path: "data/liquidation_watcher.sqlite".to_string(),
            log_level: default_log_level(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectionalGateConfig {
    #[serde(default = "default_gate_z")]
    pub long_return_z_gate: f64,
    #[serde(default = "default_volume_gate_z")]
    pub long_volume_z_gate: f64,
    #[serde(default = "default_gate_z")]
    pub short_return_z_gate: f64,
    #[serde(default = "default_volume_gate_z")]
    pub short_volume_z_gate: f64,
}

impl Default for DirectionalGateConfig {
    fn default() -> Self {
        Self {
            long_return_z_gate: default_gate_z(),
            long_volume_z_gate: default_volume_gate_z(),
            short_return_z_gate: default_gate_z(),
            short_volume_z_gate: default_volume_gate_z(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueConfig {
    #[serde(default)]
    pub binance: VenueSettings,
    #[serde(default)]
    pub bybit: VenueSettings,
    #[serde(default)]
    pub okx: VenueSettings,
    #[serde(default)]
    pub dydx: VenueSettings,
}

impl Default for VenueConfig {
    fn default() -> Self {
        Self {
            binance: VenueSettings {
                enabled: true,
                analysis_enabled: true,
                book_ticker_emit_interval_ms: 0,
                url: "wss://fstream.binance.com/stream".to_string(),
            },
            bybit: VenueSettings {
                enabled: false,
                analysis_enabled: true,
                book_ticker_emit_interval_ms: 0,
                url: "wss://stream.bybit.com/v5/public/linear".to_string(),
            },
            okx: VenueSettings {
                enabled: false,
                analysis_enabled: true,
                book_ticker_emit_interval_ms: 0,
                url: "wss://ws.okx.com:8443/ws/v5/public".to_string(),
            },
            dydx: VenueSettings {
                enabled: false,
                analysis_enabled: true,
                book_ticker_emit_interval_ms: 0,
                url: "wss://indexer.dydx.trade/v4/ws".to_string(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueSettings {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_enabled")]
    pub analysis_enabled: bool,
    #[serde(default)]
    pub book_ticker_emit_interval_ms: u64,
    pub url: String,
}

impl Default for VenueSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            analysis_enabled: true,
            book_ticker_emit_interval_ms: 0,
            url: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureWindowsConfig {
    #[serde(default = "default_1s")]
    pub return_1s: u64,
    #[serde(default = "default_5s")]
    pub return_5s: u64,
    #[serde(default = "default_15s")]
    pub return_15s: u64,
    #[serde(default = "default_30s")]
    pub return_30s: u64,
    #[serde(default = "default_state_retention")]
    pub state_retention_s: u64,
}

impl Default for FeatureWindowsConfig {
    fn default() -> Self {
        Self {
            return_1s: default_1s(),
            return_5s: default_5s(),
            return_15s: default_15s(),
            return_30s: default_30s(),
            state_retention_s: default_state_retention(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZscoreWindowsConfig {
    #[serde(default = "default_zscore_window")]
    pub seconds: usize,
}

impl Default for ZscoreWindowsConfig {
    fn default() -> Self {
        Self {
            seconds: default_zscore_window(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreWeightsConfig {
    #[serde(default = "default_return_1s_weight")]
    pub return_1s: f64,
    #[serde(default = "default_return_5s_weight")]
    pub return_5s: f64,
    #[serde(default = "default_signed_volume_1s_weight")]
    pub signed_volume_1s: f64,
    #[serde(default = "default_signed_volume_5s_weight")]
    pub signed_volume_5s: f64,
    #[serde(default = "default_trade_burst_weight")]
    pub trade_burst: f64,
    #[serde(default = "default_spread_widening_weight")]
    pub spread_widening: f64,
    #[serde(default = "default_volatility_burst_weight")]
    pub volatility_burst: f64,
    #[serde(default = "default_venue_weights")]
    pub market_venue_weights: HashMap<String, f64>,
}

impl Default for ScoreWeightsConfig {
    fn default() -> Self {
        Self {
            return_1s: default_return_1s_weight(),
            return_5s: default_return_5s_weight(),
            signed_volume_1s: default_signed_volume_1s_weight(),
            signed_volume_5s: default_signed_volume_5s_weight(),
            trade_burst: default_trade_burst_weight(),
            spread_widening: default_spread_widening_weight(),
            volatility_burst: default_volatility_burst_weight(),
            market_venue_weights: default_venue_weights(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThresholdsConfig {
    #[serde(default = "default_venue_threshold")]
    pub venue_long_threshold: f64,
    #[serde(default = "default_venue_threshold")]
    pub venue_short_threshold: f64,
    #[serde(default = "default_market_threshold")]
    pub market_long_threshold: f64,
    #[serde(default = "default_market_threshold")]
    pub market_short_threshold: f64,
    #[serde(default = "default_rearm_ratio")]
    pub venue_rearm_ratio: f64,
    #[serde(default = "default_rearm_ratio")]
    pub market_rearm_ratio: f64,
    #[serde(default = "default_market_confirmations")]
    pub market_min_confirmations: usize,
}

impl Default for ThresholdsConfig {
    fn default() -> Self {
        Self {
            venue_long_threshold: default_venue_threshold(),
            venue_short_threshold: default_venue_threshold(),
            market_long_threshold: default_market_threshold(),
            market_short_threshold: default_market_threshold(),
            venue_rearm_ratio: default_rearm_ratio(),
            market_rearm_ratio: default_rearm_ratio(),
            market_min_confirmations: default_market_confirmations(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CooldownConfig {
    #[serde(default = "default_venue_cooldown")]
    pub venue_event_s: u64,
    #[serde(default = "default_market_cooldown")]
    pub market_event_s: u64,
}

impl Default for CooldownConfig {
    fn default() -> Self {
        Self {
            venue_event_s: default_venue_cooldown(),
            market_event_s: default_market_cooldown(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationConfig {
    #[serde(default = "default_horizons")]
    pub horizons_s: Vec<u64>,
    #[serde(default = "default_success_threshold")]
    pub default_success_threshold_bps: f64,
    #[serde(default = "default_symbol_thresholds")]
    pub success_thresholds_bps: HashMap<String, f64>,
}

impl Default for EvaluationConfig {
    fn default() -> Self {
        Self {
            horizons_s: default_horizons(),
            default_success_threshold_bps: default_success_threshold(),
            success_thresholds_bps: default_symbol_thresholds(),
        }
    }
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn default_symbols() -> Vec<String> {
    vec!["BTCUSDT".to_string()]
}

fn default_venue_weights() -> HashMap<String, f64> {
    HashMap::from([
        ("binance".to_string(), 1.0),
        ("bybit".to_string(), 0.8),
        ("okx".to_string(), 0.8),
        ("dydx".to_string(), 0.3),
    ])
}

fn default_symbol_thresholds() -> HashMap<String, f64> {
    HashMap::from([
        ("BTCUSDT".to_string(), 20.0),
        ("ETHUSDT".to_string(), 25.0),
        ("SOLUSDT".to_string(), 35.0),
    ])
}

fn default_enabled() -> bool {
    true
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_online_bucket_ms() -> u64 {
    250
}

fn default_warmup_seconds() -> u64 {
    120
}

fn default_min_directional_core() -> f64 {
    2.5
}

fn default_max_stress_amplifier() -> f64 {
    2.5
}

fn default_min_return_bps() -> f64 {
    1.0
}

fn default_gate_z() -> f64 {
    0.5
}

fn default_volume_gate_z() -> f64 {
    1.0
}

fn default_1s() -> u64 {
    1
}

fn default_5s() -> u64 {
    5
}

fn default_15s() -> u64 {
    15
}

fn default_30s() -> u64 {
    30
}

fn default_state_retention() -> u64 {
    600
}

fn default_zscore_window() -> usize {
    60
}

fn default_return_1s_weight() -> f64 {
    1.2
}
fn default_return_5s_weight() -> f64 {
    1.1
}
fn default_signed_volume_1s_weight() -> f64 {
    1.0
}
fn default_signed_volume_5s_weight() -> f64 {
    0.9
}
fn default_trade_burst_weight() -> f64 {
    0.20
}
fn default_spread_widening_weight() -> f64 {
    0.05
}
fn default_volatility_burst_weight() -> f64 {
    0.15
}

fn default_venue_threshold() -> f64 {
    6.0
}

fn default_market_threshold() -> f64 {
    4.5
}

fn default_rearm_ratio() -> f64 {
    0.7
}

fn default_market_confirmations() -> usize {
    2
}

fn default_venue_cooldown() -> u64 {
    30
}

fn default_market_cooldown() -> u64 {
    30
}

fn default_horizons() -> Vec<u64> {
    vec![10, 30, 60, 120]
}

fn default_success_threshold() -> f64 {
    20.0
}
