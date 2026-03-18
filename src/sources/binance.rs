use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::{
    config::AppConfig,
    types::{MarketEvent, QuoteTick, TradeTick, Venue},
};

pub fn spawn(config: &AppConfig, tx: mpsc::Sender<MarketEvent>) -> JoinHandle<()> {
    let url = {
        let settings = &config.venues.binance;
        let streams = config
            .symbols
            .iter()
            .flat_map(|symbol| {
                let lower = symbol.to_lowercase();
                [format!("{lower}@trade"), format!("{lower}@bookTicker")]
            })
            .collect::<Vec<_>>()
            .join("/");
        format!("{}?streams={streams}", settings.url)
    };

    tokio::spawn(async move {
        loop {
            match run_once(&url, tx.clone()).await {
                Ok(()) => warn!("binance stream ended, reconnecting"),
                Err(err) => warn!(error = %err, "binance stream failed"),
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    })
}

async fn run_once(url: &str, tx: mpsc::Sender<MarketEvent>) -> anyhow::Result<()> {
    let (mut ws, _) = connect_async(url).await?;
    info!(venue = "binance", url, "source connected");

    while let Some(message) = ws.next().await {
        match message? {
            Message::Text(text) => handle_text(&text, &tx).await?,
            Message::Ping(payload) => ws.send(Message::Pong(payload)).await?,
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

async fn handle_text(text: &str, tx: &mpsc::Sender<MarketEvent>) -> anyhow::Result<()> {
    let value: Value = serde_json::from_str(text)?;
    let stream = value
        .get("stream")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let data = value.get("data").cloned().unwrap_or(value);
    let recv_time_ms = now_ms();

    if stream.ends_with("@trade") {
        let tick = TradeTick {
            symbol: data["s"].as_str().unwrap_or_default().to_string(),
            venue: Venue::Binance,
            price: parse_f64(&data["p"]),
            quantity: parse_f64(&data["q"]),
            is_buyer_maker: data["m"].as_bool().unwrap_or(false),
            event_time_ms: data["T"].as_i64().unwrap_or(recv_time_ms),
            recv_time_ms,
        };
        tx.send(MarketEvent::Trade(tick)).await?;
    } else if stream.ends_with("@bookTicker") {
        let tick = QuoteTick {
            symbol: data["s"].as_str().unwrap_or_default().to_string(),
            venue: Venue::Binance,
            bid_price: parse_f64(&data["b"]),
            bid_size: parse_f64(&data["B"]),
            ask_price: parse_f64(&data["a"]),
            ask_size: parse_f64(&data["A"]),
            event_time_ms: data["E"].as_i64().unwrap_or(recv_time_ms),
            recv_time_ms,
        };
        tx.send(MarketEvent::Quote(tick)).await?;
    }

    Ok(())
}

fn parse_f64(value: &Value) -> f64 {
    value
        .as_str()
        .and_then(|raw| raw.parse::<f64>().ok())
        .or_else(|| value.as_f64())
        .unwrap_or_default()
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}
