use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::{
    config::AppConfig,
    types::{MarketEvent, QuoteTick, TradeTick, Venue},
};

pub fn spawn(config: &AppConfig, tx: mpsc::Sender<MarketEvent>) -> JoinHandle<()> {
    let url = config.venues.bybit.url.clone();
    let symbols = config.symbols.clone();
    tokio::spawn(async move {
        loop {
            match run_once(&url, &symbols, tx.clone()).await {
                Ok(()) => warn!("bybit stream ended, reconnecting"),
                Err(err) => warn!(error = %err, "bybit stream failed"),
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    })
}

async fn run_once(
    url: &str,
    symbols: &[String],
    tx: mpsc::Sender<MarketEvent>,
) -> anyhow::Result<()> {
    let (mut ws, _) = connect_async(url).await?;
    let args = symbols
        .iter()
        .flat_map(|symbol| [format!("publicTrade.{symbol}"), format!("tickers.{symbol}")])
        .collect::<Vec<_>>();
    ws.send(Message::Text(
        json!({ "op": "subscribe", "args": args })
            .to_string()
            .into(),
    ))
    .await?;
    info!(venue = "bybit", url, "source connected");

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
    let topic = value
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let recv_time_ms = now_ms();

    if topic.starts_with("publicTrade.") {
        if let Some(items) = value.get("data").and_then(Value::as_array) {
            for item in items {
                let side = item.get("S").and_then(Value::as_str).unwrap_or("Buy");
                let tick = TradeTick {
                    symbol: item["s"].as_str().unwrap_or_default().to_string(),
                    venue: Venue::Bybit,
                    price: parse_f64(&item["p"]),
                    quantity: parse_f64(&item["v"]),
                    is_buyer_maker: matches!(side, "Sell"),
                    event_time_ms: item["T"].as_i64().unwrap_or(recv_time_ms),
                    recv_time_ms,
                };
                tx.send(MarketEvent::Trade(tick)).await?;
            }
        }
    } else if topic.starts_with("tickers.") {
        if let Some(item) = value.get("data").and_then(|data| {
            if data.is_array() {
                data.get(0)
            } else {
                Some(data)
            }
        }) {
            let tick = QuoteTick {
                symbol: item["symbol"].as_str().unwrap_or_default().to_string(),
                venue: Venue::Bybit,
                bid_price: parse_f64(&item["bid1Price"]),
                bid_size: parse_f64(&item["bid1Size"]),
                ask_price: parse_f64(&item["ask1Price"]),
                ask_size: parse_f64(&item["ask1Size"]),
                event_time_ms: value["ts"].as_i64().unwrap_or(recv_time_ms),
                recv_time_ms,
            };
            if tick.bid_price > 0.0 && tick.ask_price > 0.0 {
                tx.send(MarketEvent::Quote(tick)).await?;
            }
        }
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
