use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::{
    config::AppConfig,
    types::{Direction, LiquidationTick, MarketEvent, QuoteTick, TradeTick, Venue},
};

const INTERNAL_QUEUE_CAPACITY: usize = 10_000;
const SUMMARY_INTERVAL_SECS: u64 = 60;

#[derive(Debug, Default, Clone, Copy)]
struct BinanceSourceSnapshot {
    messages_received: u64,
    market_events_parsed: u64,
    internal_queue_dropped: u64,
    ping_received: u64,
    pong_received: u64,
    reconnect_count: u64,
}

#[derive(Debug, Default)]
struct BinanceSourceStats {
    messages_received: u64,
    market_events_parsed: u64,
    internal_queue_dropped: u64,
    ping_received: u64,
    pong_received: u64,
    reconnect_count: u64,
}

impl BinanceSourceStats {
    fn snapshot(&self) -> BinanceSourceSnapshot {
        BinanceSourceSnapshot {
            messages_received: self.messages_received,
            market_events_parsed: self.market_events_parsed,
            internal_queue_dropped: self.internal_queue_dropped,
            ping_received: self.ping_received,
            pong_received: self.pong_received,
            reconnect_count: self.reconnect_count,
        }
    }

    fn note_drop(&mut self) -> u64 {
        self.internal_queue_dropped += 1;
        self.internal_queue_dropped
    }
}

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
    let stats = Arc::new(Mutex::new(BinanceSourceStats::default()));
    let reporter_stats = Arc::clone(&stats);

    tokio::spawn(async move {
        tokio::spawn(async move {
            let mut last = BinanceSourceSnapshot::default();
            loop {
                tokio::time::sleep(Duration::from_secs(SUMMARY_INTERVAL_SECS)).await;
                let current = reporter_stats
                    .lock()
                    .expect("binance source stats lock poisoned")
                    .snapshot();
                info!(
                    venue = "binance",
                    messages_received_interval = current.messages_received - last.messages_received,
                    parsed_events_interval =
                        current.market_events_parsed - last.market_events_parsed,
                    dropped_internal_events_interval =
                        current.internal_queue_dropped - last.internal_queue_dropped,
                    ping_received_interval = current.ping_received - last.ping_received,
                    pong_received_interval = current.pong_received - last.pong_received,
                    reconnect_count_total = current.reconnect_count,
                    "binance source health"
                );
                last = current;
            }
        });

        loop {
            {
                let mut stats = stats.lock().expect("binance source stats lock poisoned");
                stats.reconnect_count += 1;
            }
            match run_once(&url, tx.clone(), Arc::clone(&stats)).await {
                Ok(()) => warn!("binance stream ended, reconnecting"),
                Err(err) => warn!(error = %err, "binance stream failed"),
            }
            // tokio::time::sleep(Duration::from_secs(3)).await;
        }
    })
}

async fn run_once(
    url: &str,
    app_tx: mpsc::Sender<MarketEvent>,
    stats: Arc<Mutex<BinanceSourceStats>>,
) -> anyhow::Result<()> {
    let (mut ws, _) = connect_async(url).await?;
    info!(venue = "binance", url, "source connected");

    let (internal_tx, mut internal_rx) = mpsc::channel::<MarketEvent>(INTERNAL_QUEUE_CAPACITY);
    let forwarder = tokio::spawn(async move {
        while let Some(event) = internal_rx.recv().await {
            if app_tx.send(event).await.is_err() {
                warn!("binance forwarder stopped: runtime channel closed");
                break;
            }
        }
    });

    // The websocket reader must never await on the shared runtime channel. It only
    // drains the socket, handles control frames, parses payloads, and try_sends
    // into an internal queue so socket liveness wins over perfect retention.
    let reader_result = async {
        while let Some(message) = ws.next().await {
            {
                let mut stats = stats.lock().expect("binance source stats lock poisoned");
                stats.messages_received += 1;
            }
            match message? {
                Message::Text(text) => handle_text(&text, &internal_tx, &stats)?,
                Message::Ping(payload) => {
                    {
                        let mut stats = stats.lock().expect("binance source stats lock poisoned");
                        stats.ping_received += 1;
                    }
                    ws.send(Message::Pong(payload)).await?;
                }
                Message::Pong(_) => {
                    let mut stats = stats.lock().expect("binance source stats lock poisoned");
                    stats.pong_received += 1;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    drop(internal_tx);
    if let Err(err) = forwarder.await {
        warn!(error = %err, "binance forwarder task join failed");
    }

    reader_result
}

fn handle_text(
    text: &str,
    internal_tx: &mpsc::Sender<MarketEvent>,
    stats: &Arc<Mutex<BinanceSourceStats>>,
) -> anyhow::Result<()> {
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
        enqueue_event(MarketEvent::Trade(tick), internal_tx, stats, &stream);
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
        enqueue_event(MarketEvent::Quote(tick), internal_tx, stats, &stream);
    } else if stream.ends_with("@forceOrder") {
        match parse_force_order(&data, recv_time_ms) {
            Ok(Some(tick)) => {
                info!(
                    symbol = %tick.symbol,
                    side = tick.side.as_str(),
                    price = tick.price,
                    quantity = tick.quantity,
                    "binance liquidation event received"
                );
                enqueue_event(MarketEvent::Liquidation(tick), internal_tx, stats, &stream);
            }
            Ok(None) => {}
            Err(err) => {
                warn!(error = %err, stream, "failed to parse binance forceOrder payload");
            }
        }
    }

    Ok(())
}

fn enqueue_event(
    event: MarketEvent,
    internal_tx: &mpsc::Sender<MarketEvent>,
    stats: &Arc<Mutex<BinanceSourceStats>>,
    stream: &str,
) {
    match internal_tx.try_send(event) {
        Ok(()) => {
            let mut stats = stats.lock().expect("binance source stats lock poisoned");
            stats.market_events_parsed += 1;
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            let dropped = {
                let mut stats = stats.lock().expect("binance source stats lock poisoned");
                stats.note_drop()
            };
            if dropped <= 3 || dropped % 10_000 == 0 {
                warn!(
                    venue = "binance",
                    stream,
                    dropped_total = dropped,
                    "binance internal queue full, dropping parsed market event"
                );
            }
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            warn!(venue = "binance", stream, "binance internal queue closed");
        }
    }
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
