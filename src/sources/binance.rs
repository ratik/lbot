use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::{
    config::AppConfig,
    types::{Direction, LiquidationTick, MarketEvent, QuoteTick, TradeTick, Venue},
};

pub fn spawn(config: &AppConfig, tx: mpsc::Sender<MarketEvent>) -> JoinHandle<()> {
    let url = {
        let settings = &config.venues.binance;
        let streams = config
            .symbols
            .iter()
            .flat_map(|symbol| {
                let lower = symbol.to_lowercase();
                [
                    format!("{lower}@trade"),
                    format!("{lower}@bookTicker"),
                    format!("{lower}@forceOrder"),
                ]
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
    } else if stream.ends_with("@forceOrder") {
        match parse_force_order(&data, recv_time_ms) {
            Ok(Some(tick)) => {
                debug!(
                    symbol = %tick.symbol,
                    side = tick.side.as_str(),
                    price = tick.price,
                    quantity = tick.quantity,
                    "binance liquidation event received"
                );
                tx.send(MarketEvent::Liquidation(tick)).await?;
            }
            Ok(None) => {}
            Err(err) => {
                warn!(error = %err, stream, "failed to parse binance forceOrder payload");
            }
        }
    }

    Ok(())
}

fn parse_force_order(data: &Value, recv_time_ms: i64) -> anyhow::Result<Option<LiquidationTick>> {
    let order = match data.get("o") {
        Some(order) => order,
        None => return Ok(None),
    };
    let symbol = order
        .get("s")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing liquidation symbol"))?;
    let side_raw = order
        .get("S")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing liquidation side"))?;
    let side = match side_raw {
        "SELL" => Direction::Long,
        "BUY" => Direction::Short,
        other => return Err(anyhow::anyhow!("unexpected liquidation side {other}")),
    };
    let price = parse_f64(&order["p"]);
    let quantity = parse_f64(&order["q"]);
    let event_time_ms = data["E"].as_i64().unwrap_or(recv_time_ms);
    if price <= 0.0 || quantity <= 0.0 {
        return Err(anyhow::anyhow!(
            "invalid liquidation price/quantity: price={price}, quantity={quantity}"
        ));
    }

    Ok(Some(LiquidationTick {
        symbol: symbol.to_string(),
        venue: Venue::Binance,
        recv_time_ms,
        event_time_ms,
        side,
        price,
        quantity,
    }))
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
