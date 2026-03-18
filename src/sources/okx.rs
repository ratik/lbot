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
    let url = config.venues.okx.url.clone();
    let symbols = config.symbols.clone();
    tokio::spawn(async move {
        loop {
            match run_once(&url, &symbols, tx.clone()).await {
                Ok(()) => warn!("okx stream ended, reconnecting"),
                Err(err) => warn!(error = %err, "okx stream failed"),
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
        .flat_map(|symbol| {
            let inst = okx_symbol(symbol);
            [
                json!({ "channel": "trades", "instId": inst }),
                json!({ "channel": "tickers", "instId": inst }),
            ]
        })
        .collect::<Vec<_>>();
    ws.send(Message::Text(
        json!({ "op": "subscribe", "args": args })
            .to_string()
            .into(),
    ))
    .await?;
    info!(venue = "okx", url, "source connected");

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
    let channel = value
        .get("arg")
        .and_then(|arg| arg.get("channel"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let recv_time_ms = now_ms();

    if channel == "trades" {
        if let Some(items) = value.get("data").and_then(Value::as_array) {
            for item in items {
                let tick = TradeTick {
                    symbol: canonical_symbol(item["instId"].as_str().unwrap_or_default()),
                    venue: Venue::Okx,
                    price: parse_f64(&item["px"]),
                    quantity: parse_f64(&item["sz"]),
                    is_buyer_maker: matches!(item["side"].as_str().unwrap_or("buy"), "sell"),
                    event_time_ms: parse_i64(&item["ts"]).unwrap_or(recv_time_ms),
                    recv_time_ms,
                };
                tx.send(MarketEvent::Trade(tick)).await?;
            }
        }
    } else if channel == "tickers" {
        if let Some(items) = value.get("data").and_then(Value::as_array) {
            for item in items {
                let tick = QuoteTick {
                    symbol: canonical_symbol(item["instId"].as_str().unwrap_or_default()),
                    venue: Venue::Okx,
                    bid_price: parse_f64(&item["bidPx"]),
                    bid_size: parse_f64(&item["bidSz"]),
                    ask_price: parse_f64(&item["askPx"]),
                    ask_size: parse_f64(&item["askSz"]),
                    event_time_ms: parse_i64(&item["ts"]).unwrap_or(recv_time_ms),
                    recv_time_ms,
                };
                if tick.bid_price > 0.0 && tick.ask_price > 0.0 {
                    tx.send(MarketEvent::Quote(tick)).await?;
                }
            }
        }
    }
    Ok(())
}

fn okx_symbol(symbol: &str) -> String {
    match symbol {
        "BTCUSDT" => "BTC-USDT-SWAP".to_string(),
        "ETHUSDT" => "ETH-USDT-SWAP".to_string(),
        "SOLUSDT" => "SOL-USDT-SWAP".to_string(),
        other => {
            let base = other.strip_suffix("USDT").unwrap_or(other);
            format!("{base}-USDT-SWAP")
        }
    }
}

fn canonical_symbol(inst_id: &str) -> String {
    inst_id.replace("-USDT-SWAP", "USDT").replace('-', "")
}

fn parse_f64(value: &Value) -> f64 {
    value
        .as_str()
        .and_then(|raw| raw.parse::<f64>().ok())
        .or_else(|| value.as_f64())
        .unwrap_or_default()
}

fn parse_i64(value: &Value) -> Option<i64> {
    value
        .as_str()
        .and_then(|raw| raw.parse::<i64>().ok())
        .or_else(|| value.as_i64())
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}
