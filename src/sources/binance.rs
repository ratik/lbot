use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
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
const SUMMARY_INTERVAL_SECS: u64 = 10;

#[derive(Debug, Clone, Copy)]
enum StreamKind {
    Trade,
    BookTicker,
    ForceOrder,
}

#[derive(Debug)]
enum ParsedEvent {
    Trade(TradeTick),
    BookTicker(QuoteTick),
    ForceOrder(LiquidationTick),
}

#[derive(Debug, Default)]
struct BinanceSourceStats {
    messages_received: AtomicU64,
    ping_received: AtomicU64,
    pong_received: AtomicU64,
    reconnect_count: AtomicU64,
    trade_received: AtomicU64,
    trade_forwarded: AtomicU64,
    trade_dropped: AtomicU64,
    book_ticker_received: AtomicU64,
    book_ticker_forwarded: AtomicU64,
    book_ticker_dropped: AtomicU64,
    book_ticker_coalesced: AtomicU64,
    force_order_received: AtomicU64,
    force_order_forwarded: AtomicU64,
    force_order_dropped: AtomicU64,
    internal_queue_depth: AtomicI64,
}

impl BinanceSourceStats {
    fn note_received(&self, kind: StreamKind) {
        match kind {
            StreamKind::Trade => {
                self.trade_received.fetch_add(1, Ordering::Relaxed);
            }
            StreamKind::BookTicker => {
                self.book_ticker_received.fetch_add(1, Ordering::Relaxed);
            }
            StreamKind::ForceOrder => {
                self.force_order_received.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn note_forwarded(&self, kind: StreamKind) {
        match kind {
            StreamKind::Trade => {
                self.trade_forwarded.fetch_add(1, Ordering::Relaxed);
            }
            StreamKind::BookTicker => {
                self.book_ticker_forwarded.fetch_add(1, Ordering::Relaxed);
            }
            StreamKind::ForceOrder => {
                self.force_order_forwarded.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn note_dropped(&self, kind: StreamKind) -> u64 {
        match kind {
            StreamKind::Trade => self.trade_dropped.fetch_add(1, Ordering::Relaxed) + 1,
            StreamKind::BookTicker => self.book_ticker_dropped.fetch_add(1, Ordering::Relaxed) + 1,
            StreamKind::ForceOrder => self.force_order_dropped.fetch_add(1, Ordering::Relaxed) + 1,
        }
    }

    fn note_book_ticker_coalesced(&self) {
        self.book_ticker_coalesced.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct PendingBookTicker {
    pending_tick: Option<QuoteTick>,
    last_emitted_ms: i64,
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
    let book_ticker_emit_interval_ms = config.venues.binance.book_ticker_emit_interval_ms;
    let stats = Arc::new(BinanceSourceStats::default());
    let reporter_stats = Arc::clone(&stats);

    tokio::spawn(async move {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(SUMMARY_INTERVAL_SECS)).await;

                let messages_received = reporter_stats.messages_received.swap(0, Ordering::Relaxed);
                let trade_received = reporter_stats.trade_received.swap(0, Ordering::Relaxed);
                let trade_forwarded = reporter_stats.trade_forwarded.swap(0, Ordering::Relaxed);
                let trade_dropped = reporter_stats.trade_dropped.swap(0, Ordering::Relaxed);
                let book_ticker_received = reporter_stats
                    .book_ticker_received
                    .swap(0, Ordering::Relaxed);
                let book_ticker_forwarded = reporter_stats
                    .book_ticker_forwarded
                    .swap(0, Ordering::Relaxed);
                let book_ticker_dropped = reporter_stats
                    .book_ticker_dropped
                    .swap(0, Ordering::Relaxed);
                let book_ticker_coalesced = reporter_stats
                    .book_ticker_coalesced
                    .swap(0, Ordering::Relaxed);
                let force_order_received = reporter_stats
                    .force_order_received
                    .swap(0, Ordering::Relaxed);
                let force_order_forwarded = reporter_stats
                    .force_order_forwarded
                    .swap(0, Ordering::Relaxed);
                let force_order_dropped = reporter_stats
                    .force_order_dropped
                    .swap(0, Ordering::Relaxed);
                let ping_received = reporter_stats.ping_received.swap(0, Ordering::Relaxed);
                let pong_received = reporter_stats.pong_received.swap(0, Ordering::Relaxed);
                let reconnect_count_total = reporter_stats.reconnect_count.load(Ordering::Relaxed);
                let internal_queue_len = reporter_stats
                    .internal_queue_depth
                    .load(Ordering::Relaxed)
                    .max(0);
                let dropped_total_interval =
                    trade_dropped + book_ticker_dropped + force_order_dropped;

                let trade_drop_rate = rate(trade_dropped, trade_received);
                let book_ticker_drop_rate = rate(book_ticker_dropped, book_ticker_received);
                let force_order_drop_rate = rate(force_order_dropped, force_order_received);

                if dropped_total_interval > 0 {
                    warn!(
                        venue = "binance",
                        interval_s = SUMMARY_INTERVAL_SECS,
                        book_ticker_emit_interval_ms,
                        messages_received,
                        trade_received,
                        trade_forwarded,
                        trade_dropped,
                        trade_drop_rate,
                        book_ticker_received,
                        book_ticker_forwarded,
                        book_ticker_dropped,
                        book_ticker_coalesced,
                        book_ticker_drop_rate,
                        force_order_received,
                        force_order_forwarded,
                        force_order_dropped,
                        force_order_drop_rate,
                        dropped_total_interval,
                        ping_received,
                        pong_received,
                        internal_queue_capacity = INTERNAL_QUEUE_CAPACITY,
                        internal_queue_len,
                        reconnect_count_total,
                        "binance stream stats"
                    );
                } else {
                    info!(
                        venue = "binance",
                        interval_s = SUMMARY_INTERVAL_SECS,
                        book_ticker_emit_interval_ms,
                        messages_received,
                        trade_received,
                        trade_forwarded,
                        trade_dropped,
                        trade_drop_rate,
                        book_ticker_received,
                        book_ticker_forwarded,
                        book_ticker_dropped,
                        book_ticker_coalesced,
                        book_ticker_drop_rate,
                        force_order_received,
                        force_order_forwarded,
                        force_order_dropped,
                        force_order_drop_rate,
                        dropped_total_interval,
                        ping_received,
                        pong_received,
                        internal_queue_capacity = INTERNAL_QUEUE_CAPACITY,
                        internal_queue_len,
                        reconnect_count_total,
                        "binance stream stats"
                    );
                }
            }
        });

        loop {
            stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
            match run_once(
                &url,
                tx.clone(),
                Arc::clone(&stats),
                book_ticker_emit_interval_ms,
            )
            .await
            {
                Ok(()) => warn!("binance stream ended, reconnecting"),
                Err(err) => warn!(error = %err, "binance stream failed"),
            }
        }
    })
}

async fn run_once(
    url: &str,
    app_tx: mpsc::Sender<MarketEvent>,
    stats: Arc<BinanceSourceStats>,
    book_ticker_emit_interval_ms: u64,
) -> anyhow::Result<()> {
    let (mut ws, _) = connect_async(url).await?;
    info!(venue = "binance", url, "source connected");

    let (internal_tx, mut internal_rx) = mpsc::channel::<MarketEvent>(INTERNAL_QUEUE_CAPACITY);
    let forwarder_stats = Arc::clone(&stats);
    let forwarder = tokio::spawn(async move {
        while let Some(event) = internal_rx.recv().await {
            forwarder_stats
                .internal_queue_depth
                .fetch_sub(1, Ordering::Relaxed);
            if app_tx.send(event).await.is_err() {
                warn!("binance forwarder stopped: runtime channel closed");
                break;
            }
        }
    });

    let throttling_enabled = book_ticker_emit_interval_ms > 0;
    let mut pending_book_tickers: HashMap<String, PendingBookTicker> = HashMap::new();
    let mut flush_interval = if throttling_enabled {
        Some(tokio::time::interval(Duration::from_millis(
            book_ticker_emit_interval_ms,
        )))
    } else {
        None
    };

    // The websocket reader must never await on the shared runtime channel. It only
    // drains the socket, handles control frames, parses payloads, and try_sends
    // into an internal queue so socket liveness wins over perfect retention.
    let reader_result = async {
        loop {
            if let Some(interval) = flush_interval.as_mut() {
                tokio::select! {
                    maybe_message = ws.next() => {
                        let Some(message) = maybe_message else { break; };
                        stats.messages_received.fetch_add(1, Ordering::Relaxed);
                        match message? {
                            Message::Text(text) => {
                                if let Some(parsed) = parse_text(&text)? {
                                    handle_parsed_event(
                                        parsed,
                                        &internal_tx,
                                        &stats,
                                        &mut pending_book_tickers,
                                        book_ticker_emit_interval_ms as i64,
                                    );
                                }
                            }
                            Message::Ping(payload) => {
                                stats.ping_received.fetch_add(1, Ordering::Relaxed);
                                ws.send(Message::Pong(payload)).await?;
                            }
                            Message::Pong(_) => {
                                stats.pong_received.fetch_add(1, Ordering::Relaxed);
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                    _ = interval.tick() => {
                        flush_pending_book_tickers(
                            &internal_tx,
                            &stats,
                            &mut pending_book_tickers,
                            book_ticker_emit_interval_ms as i64,
                        );
                    }
                }
            } else {
                let Some(message) = ws.next().await else {
                    break;
                };
                stats.messages_received.fetch_add(1, Ordering::Relaxed);
                match message? {
                    Message::Text(text) => {
                        if let Some(parsed) = parse_text(&text)? {
                            handle_parsed_event(
                                parsed,
                                &internal_tx,
                                &stats,
                                &mut pending_book_tickers,
                                0,
                            );
                        }
                    }
                    Message::Ping(payload) => {
                        stats.ping_received.fetch_add(1, Ordering::Relaxed);
                        ws.send(Message::Pong(payload)).await?;
                    }
                    Message::Pong(_) => {
                        stats.pong_received.fetch_add(1, Ordering::Relaxed);
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
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

fn parse_text(text: &str) -> anyhow::Result<Option<ParsedEvent>> {
    let value: Value = serde_json::from_str(text)?;
    let stream = value
        .get("stream")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = value.get("data").unwrap_or(&value);
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
        Ok(Some(ParsedEvent::Trade(tick)))
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
        Ok(Some(ParsedEvent::BookTicker(tick)))
    } else if stream.ends_with("@forceOrder") {
        match parse_force_order(data, recv_time_ms) {
            Ok(Some(tick)) => {
                info!(
                    symbol = %tick.symbol,
                    side = tick.side.as_str(),
                    price = tick.price,
                    quantity = tick.quantity,
                    "binance liquidation event received"
                );
                Ok(Some(ParsedEvent::ForceOrder(tick)))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                warn!(error = %err, stream, "failed to parse binance forceOrder payload");
                Ok(None)
            }
        }
    } else {
        Ok(None)
    }
}

fn handle_parsed_event(
    parsed: ParsedEvent,
    internal_tx: &mpsc::Sender<MarketEvent>,
    stats: &Arc<BinanceSourceStats>,
    pending_book_tickers: &mut HashMap<String, PendingBookTicker>,
    book_ticker_emit_interval_ms: i64,
) {
    match parsed {
        ParsedEvent::Trade(tick) => {
            stats.note_received(StreamKind::Trade);
            enqueue_event(
                MarketEvent::Trade(tick),
                internal_tx,
                stats,
                StreamKind::Trade,
                "trade",
            );
        }
        ParsedEvent::ForceOrder(tick) => {
            stats.note_received(StreamKind::ForceOrder);
            enqueue_event(
                MarketEvent::Liquidation(tick),
                internal_tx,
                stats,
                StreamKind::ForceOrder,
                "forceOrder",
            );
        }
        ParsedEvent::BookTicker(tick) => {
            stats.note_received(StreamKind::BookTicker);
            if book_ticker_emit_interval_ms <= 0 {
                enqueue_event(
                    MarketEvent::Quote(tick),
                    internal_tx,
                    stats,
                    StreamKind::BookTicker,
                    "bookTicker",
                );
                return;
            }

            let symbol = tick.symbol.clone();
            if let Some(pending) = pending_book_tickers.get_mut(&symbol) {
                let replace_pending = pending
                    .pending_tick
                    .as_ref()
                    .map(|current| tick.recv_time_ms >= current.recv_time_ms)
                    .unwrap_or(true);
                if replace_pending {
                    pending.pending_tick = Some(tick);
                }
                stats.note_book_ticker_coalesced();
            } else {
                pending_book_tickers.insert(
                    symbol,
                    PendingBookTicker {
                        pending_tick: Some(tick),
                        last_emitted_ms: i64::MIN / 2,
                    },
                );
            }
            flush_pending_book_tickers(
                internal_tx,
                stats,
                pending_book_tickers,
                book_ticker_emit_interval_ms,
            );
        }
    }
}

fn flush_pending_book_tickers(
    internal_tx: &mpsc::Sender<MarketEvent>,
    stats: &Arc<BinanceSourceStats>,
    pending_book_tickers: &mut HashMap<String, PendingBookTicker>,
    book_ticker_emit_interval_ms: i64,
) {
    if book_ticker_emit_interval_ms <= 0 {
        return;
    }

    let now = now_ms();
    let ready = pending_book_tickers
        .iter()
        .filter_map(|(symbol, pending)| {
            if pending.pending_tick.is_some()
                && now - pending.last_emitted_ms >= book_ticker_emit_interval_ms
            {
                Some(symbol.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    for symbol in ready {
        if let Some(pending) = pending_book_tickers.get_mut(&symbol) {
            pending.last_emitted_ms = now;
            if let Some(tick) = pending.pending_tick.take() {
                enqueue_event(
                    MarketEvent::Quote(tick),
                    internal_tx,
                    stats,
                    StreamKind::BookTicker,
                    "bookTicker",
                );
            }
        }
    }
}

fn enqueue_event(
    event: MarketEvent,
    internal_tx: &mpsc::Sender<MarketEvent>,
    stats: &Arc<BinanceSourceStats>,
    kind: StreamKind,
    stream: &str,
) {
    match internal_tx.try_send(event) {
        Ok(()) => {
            stats.note_forwarded(kind);
            stats.internal_queue_depth.fetch_add(1, Ordering::Relaxed);
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            let dropped = stats.note_dropped(kind);
            if dropped <= 3 || dropped % 10_000 == 0 {
                warn!(
                    venue = "binance",
                    stream,
                    dropped_total_for_stream = dropped,
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

fn rate(dropped: u64, received: u64) -> f64 {
    if received == 0 {
        0.0
    } else {
        dropped as f64 / received as f64
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}
