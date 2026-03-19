use std::{
    fs,
    path::Path,
    sync::mpsc::{self, Receiver, Sender},
    thread,
};

use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use tracing::error;

use crate::types::{
    HeartbeatEvent, LiquidationOutcomeCheckEvent, LiquidationTick, MarketSignalEvent,
    OutcomeCheckEvent, VenueSignalEvent,
};

#[derive(Debug)]
enum PersistCommand {
    VenueSignal(VenueSignalEvent),
    MarketSignal(MarketSignalEvent),
    Liquidation(LiquidationTick),
    Outcome(OutcomeCheckEvent),
    LiquidationOutcome(LiquidationOutcomeCheckEvent),
    Heartbeat(HeartbeatEvent),
}

#[derive(Debug, Clone)]
pub struct PersistenceHandle {
    tx: Sender<PersistCommand>,
}

impl PersistenceHandle {
    pub fn new(path: &str) -> Result<Self> {
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("failed creating sqlite directory {}", parent.display())
                })?;
            }
        }

        let (tx, rx) = mpsc::channel();
        let db_path = path.to_string();
        thread::spawn(move || {
            if let Err(err) = run_worker(&db_path, rx) {
                error!(error = %err, "persistence worker terminated");
            }
        });
        Ok(Self { tx })
    }

    pub fn send_venue_signal(&self, event: VenueSignalEvent) -> Result<()> {
        self.tx
            .send(PersistCommand::VenueSignal(event))
            .context("venue signal send failed")
    }

    pub fn send_market_signal(&self, event: MarketSignalEvent) -> Result<()> {
        self.tx
            .send(PersistCommand::MarketSignal(event))
            .context("market signal send failed")
    }

    pub fn send_liquidation_event(&self, event: LiquidationTick) -> Result<()> {
        self.tx
            .send(PersistCommand::Liquidation(event))
            .context("liquidation send failed")
    }

    pub fn send_outcome(&self, event: OutcomeCheckEvent) -> Result<()> {
        self.tx
            .send(PersistCommand::Outcome(event))
            .context("outcome send failed")
    }

    pub fn send_liquidation_outcome(&self, event: LiquidationOutcomeCheckEvent) -> Result<()> {
        self.tx
            .send(PersistCommand::LiquidationOutcome(event))
            .context("liquidation outcome send failed")
    }

    pub fn send_heartbeat(&self, event: HeartbeatEvent) -> Result<()> {
        self.tx
            .send(PersistCommand::Heartbeat(event))
            .context("heartbeat send failed")
    }
}

fn run_worker(path: &str, rx: Receiver<PersistCommand>) -> Result<()> {
    let conn = Connection::open(path).with_context(|| format!("failed opening sqlite {}", path))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    init_schema(&conn)?;

    while let Ok(cmd) = rx.recv() {
        match cmd {
            PersistCommand::VenueSignal(event) => insert_venue_signal(&conn, &event)?,
            PersistCommand::MarketSignal(event) => insert_market_signal(&conn, &event)?,
            PersistCommand::Liquidation(event) => insert_liquidation_event(&conn, &event)?,
            PersistCommand::Outcome(event) => insert_outcome(&conn, &event)?,
            PersistCommand::LiquidationOutcome(event) => insert_liquidation_outcome(&conn, &event)?,
            PersistCommand::Heartbeat(event) => insert_heartbeat(&conn, &event)?,
        }
    }
    Ok(())
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS venue_signals (
            event_id TEXT PRIMARY KEY,
            event_time INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            venue TEXT NOT NULL,
            direction TEXT NOT NULL,
            score REAL NOT NULL,
            price REAL NOT NULL,
            spread_bps REAL NOT NULL,
            feature_json TEXT NOT NULL,
            cooldown_state TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS market_signals (
            event_id TEXT PRIMARY KEY,
            event_time INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            combined_score REAL NOT NULL,
            confirming_venues_json TEXT NOT NULL,
            first_triggered_venue TEXT NOT NULL,
            venue_spread_ms INTEGER NOT NULL,
            feature_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS outcome_checks (
            event_id TEXT NOT NULL,
            horizon_s INTEGER NOT NULL,
            return_bps REAL NOT NULL,
            max_favorable_bps REAL NOT NULL,
            max_adverse_bps REAL NOT NULL,
            realized_vol_after REAL NOT NULL,
            success_flag INTEGER NOT NULL,
            PRIMARY KEY (event_id, horizon_s)
        );

        CREATE TABLE IF NOT EXISTS liquidation_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue TEXT NOT NULL,
            symbol TEXT NOT NULL,
            recv_time_ms INTEGER NOT NULL,
            event_time_ms INTEGER NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            quantity REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_liquidation_events_symbol_time
            ON liquidation_events(symbol, event_time_ms);

        CREATE INDEX IF NOT EXISTS idx_liquidation_events_venue_symbol_time
            ON liquidation_events(venue, symbol, event_time_ms);

        CREATE TABLE IF NOT EXISTS liquidation_outcome_checks (
            event_id TEXT NOT NULL,
            horizon_s INTEGER NOT NULL,
            expected_liq_side TEXT NOT NULL,
            price_success_flag INTEGER NOT NULL,
            liq_success_flag INTEGER NOT NULL,
            liq_event_count INTEGER NOT NULL,
            liq_qty_sum REAL NOT NULL,
            liq_max_qty REAL NOT NULL,
            liq_first_seen_time_ms INTEGER,
            time_to_first_liq_ms INTEGER,
            PRIMARY KEY (event_id, horizon_s)
        );

        CREATE TABLE IF NOT EXISTS service_heartbeats (
            service_time INTEGER NOT NULL,
            status TEXT NOT NULL,
            notes TEXT NOT NULL
        );
        "#,
    )?;
    Ok(())
}

fn insert_venue_signal(conn: &Connection, event: &VenueSignalEvent) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO venue_signals
        (event_id, event_time, symbol, venue, direction, score, price, spread_bps, feature_json, cooldown_state)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            event.event_id,
            event.event_time_ms,
            event.symbol,
            event.venue.as_str(),
            event.direction.as_str(),
            event.score,
            event.price,
            event.spread_bps,
            event.feature_json,
            event.cooldown_state,
        ],
    )?;
    Ok(())
}

fn insert_market_signal(conn: &Connection, event: &MarketSignalEvent) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO market_signals
        (event_id, event_time, symbol, direction, combined_score, confirming_venues_json, first_triggered_venue, venue_spread_ms, feature_json)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            event.event_id,
            event.event_time_ms,
            event.symbol,
            event.direction.as_str(),
            event.combined_score,
            event.confirming_venues_json,
            event.first_triggered_venue,
            event.venue_spread_ms,
            event.feature_json,
        ],
    )?;
    Ok(())
}

fn insert_outcome(conn: &Connection, event: &OutcomeCheckEvent) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO outcome_checks
        (event_id, horizon_s, return_bps, max_favorable_bps, max_adverse_bps, realized_vol_after, success_flag)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            event.event_id,
            event.horizon_s as i64,
            event.return_bps,
            event.max_favorable_bps,
            event.max_adverse_bps,
            event.realized_vol_after,
            event.success_flag as i64,
        ],
    )?;
    Ok(())
}

fn insert_liquidation_event(conn: &Connection, event: &LiquidationTick) -> Result<()> {
    conn.execute(
        "INSERT INTO liquidation_events
        (venue, symbol, recv_time_ms, event_time_ms, side, price, quantity)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            event.venue.as_str(),
            event.symbol,
            event.recv_time_ms,
            event.event_time_ms,
            event.side.as_str(),
            event.price,
            event.quantity,
        ],
    )?;
    Ok(())
}

fn insert_liquidation_outcome(
    conn: &Connection,
    event: &LiquidationOutcomeCheckEvent,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO liquidation_outcome_checks
        (event_id, horizon_s, expected_liq_side, price_success_flag, liq_success_flag, liq_event_count, liq_qty_sum, liq_max_qty, liq_first_seen_time_ms, time_to_first_liq_ms)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            event.event_id,
            event.horizon_s as i64,
            event.expected_liq_side.as_str(),
            event.price_success_flag as i64,
            event.liq_success_flag as i64,
            event.liq_event_count as i64,
            event.liq_qty_sum,
            event.liq_max_qty,
            event.liq_first_seen_time_ms,
            event.time_to_first_liq_ms,
        ],
    )?;
    Ok(())
}

fn insert_heartbeat(conn: &Connection, event: &HeartbeatEvent) -> Result<()> {
    conn.execute(
        "INSERT INTO service_heartbeats (service_time, status, notes) VALUES (?1, ?2, ?3)",
        params![event.service_time_ms, event.status, event.notes],
    )?;
    Ok(())
}
