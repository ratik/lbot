use std::collections::HashMap;

use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    config::{PaperConfig, PaperEntryConfirmationMode},
    persistence::PersistenceHandle,
    types::{Direction, MarketEvent, PaperPositionRecord},
};

const SUMMARY_INTERVAL_MS: i64 = 60_000;

#[derive(Debug, Clone)]
struct PaperPosition {
    id: String,
    symbol: String,
    direction: Direction,
    entry_price: f64,
    entry_time_ms: i64,
    size: f64,
    leverage: f64,
    notional_value: f64,
    margin_used: f64,
    liquidation_price: Option<f64>,
    take_profit_bps: Option<f64>,
    stop_loss_bps: Option<f64>,
    max_duration_ms: i64,
    exit_price: Option<f64>,
    exit_time_ms: Option<i64>,
    exit_reason: Option<ExitReason>,
    realized_pnl: f64,
    max_unrealized_pnl: f64,
    min_unrealized_pnl: f64,
}

#[derive(Debug, Clone)]
struct PendingEntry {
    id: String,
    signal_event_id: String,
    symbol: String,
    direction: Direction,
    signal_time_ms: i64,
    scheduled_entry_time_ms: i64,
    signal_reference_price: f64,
}

#[derive(Debug, Clone, Copy)]
enum ExitReason {
    TakeProfit,
    StopLoss,
    Timeout,
    Liquidation,
}

impl ExitReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::TakeProfit => "take_profit",
            Self::StopLoss => "stop_loss",
            Self::Timeout => "timeout",
            Self::Liquidation => "liquidation",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct MarketSnapshot {
    price: f64,
    time_ms: i64,
}

#[derive(Debug, Default)]
pub struct PaperStats {
    total_positions: u64,
    closed_positions: u64,
    wins: u64,
    losses: u64,
    total_pnl: f64,
    total_duration_ms: i64,
    total_win_pnl: f64,
    total_loss_pnl: f64,
    win_count: u64,
    loss_count: u64,
    pending_entries_created: u64,
    entries_opened: u64,
    entries_rejected_confirmation: u64,
    entries_rejected_no_price: u64,
}

pub struct PaperEngine {
    config: PaperConfig,
    persistence: PersistenceHandle,
    active_positions: Vec<PaperPosition>,
    pending_entries: Vec<PendingEntry>,
    latest_market: HashMap<String, MarketSnapshot>,
    stats: PaperStats,
    last_summary_ms: i64,
    balance: f64,
    equity: f64,
    used_margin: f64,
    free_margin: f64,
}

impl PaperEngine {
    pub fn new(config: PaperConfig, persistence: PersistenceHandle) -> Self {
        let initial_balance = config.initial_balance;
        Self {
            config,
            persistence,
            active_positions: Vec::new(),
            pending_entries: Vec::new(),
            latest_market: HashMap::new(),
            stats: PaperStats::default(),
            last_summary_ms: 0,
            balance: initial_balance,
            equity: initial_balance,
            used_margin: 0.0,
            free_margin: initial_balance,
        }
    }

    pub fn on_market_event(&mut self, event: &MarketEvent) {
        if !self.config.enabled {
            return;
        }

        let snapshot = match event {
            MarketEvent::Quote(tick) => Some(MarketSnapshot {
                price: tick.mid_price(),
                time_ms: tick.recv_time_ms,
            }),
            MarketEvent::Trade(tick) => Some(MarketSnapshot {
                price: tick.price,
                time_ms: tick.recv_time_ms,
            }),
            MarketEvent::Liquidation(_) => None,
        };

        let Some(snapshot) = snapshot else {
            return;
        };
        if snapshot.price <= 0.0 {
            return;
        }

        self.latest_market
            .insert(event.symbol().to_string(), snapshot);
        self.process_pending_entries(event.symbol(), snapshot);
        self.update_positions_for_symbol(event.symbol(), snapshot);
        self.refresh_account_metrics();
        self.maybe_log_stats(snapshot.time_ms);
    }

    pub fn on_signal(
        &mut self,
        signal_event_id: &str,
        symbol: &str,
        direction: Direction,
        signal_time_ms: i64,
    ) {
        if !self.config.enabled {
            return;
        }

        let Some(snapshot) = self.latest_market.get(symbol).copied() else {
            self.stats.entries_rejected_no_price += 1;
            info!(
                target: "paper",
                signal_event_id,
                symbol,
                direction = direction.as_str(),
                rejection_reason = "no_signal_reference_price",
                "pending entry rejected"
            );
            return;
        };
        if snapshot.price <= 0.0 {
            self.stats.entries_rejected_no_price += 1;
            info!(
                target: "paper",
                signal_event_id,
                symbol,
                direction = direction.as_str(),
                current_price = snapshot.price,
                rejection_reason = "invalid_signal_reference_price",
                "pending entry rejected"
            );
            return;
        }

        let pending = PendingEntry {
            id: Uuid::new_v4().to_string(),
            signal_event_id: signal_event_id.to_string(),
            symbol: symbol.to_string(),
            direction,
            signal_time_ms,
            scheduled_entry_time_ms: signal_time_ms + self.config.entry_delay_ms as i64,
            signal_reference_price: snapshot.price,
        };
        self.stats.pending_entries_created += 1;

        info!(
            target: "paper",
            signal_event_id = %pending.signal_event_id,
            symbol = %pending.symbol,
            direction = pending.direction.as_str(),
            signal_time_ms = pending.signal_time_ms,
            signal_reference_price = pending.signal_reference_price,
            scheduled_entry_time_ms = pending.scheduled_entry_time_ms,
            entry_delay_ms = self.config.entry_delay_ms,
            confirmation_mode = confirmation_mode_str(self.config.entry_confirmation_mode),
            "pending entry created"
        );

        self.pending_entries.push(pending);
    }

    pub fn maybe_log_stats(&mut self, now_ms: i64) {
        if !self.config.enabled {
            return;
        }
        if self.last_summary_ms > 0 && now_ms - self.last_summary_ms < SUMMARY_INTERVAL_MS {
            return;
        }
        self.last_summary_ms = now_ms;

        let closed_positions = self.stats.closed_positions;
        let wins = self.stats.wins;
        let win_rate = if closed_positions == 0 {
            0.0
        } else {
            wins as f64 / closed_positions as f64
        };
        let avg_pnl = if closed_positions == 0 {
            0.0
        } else {
            self.stats.total_pnl / closed_positions as f64
        };
        let avg_win = if self.stats.win_count == 0 {
            0.0
        } else {
            self.stats.total_win_pnl / self.stats.win_count as f64
        };
        let avg_loss = if self.stats.loss_count == 0 {
            0.0
        } else {
            self.stats.total_loss_pnl / self.stats.loss_count as f64
        };
        let avg_duration_ms = if closed_positions == 0 {
            0.0
        } else {
            self.stats.total_duration_ms as f64 / closed_positions as f64
        };
        let open_rate = if self.stats.pending_entries_created == 0 {
            0.0
        } else {
            self.stats.entries_opened as f64 / self.stats.pending_entries_created as f64
        };

        info!(
            target: "paper",
            open_positions = self.active_positions.len(),
            pending_entries = self.pending_entries.len(),
            total_positions = self.stats.total_positions,
            closed_positions,
            wins = self.stats.wins,
            losses = self.stats.losses,
            win_rate,
            total_pnl = self.stats.total_pnl,
            avg_pnl,
            avg_win,
            avg_loss,
            avg_duration_ms,
            pending_entries_created = self.stats.pending_entries_created,
            entries_opened = self.stats.entries_opened,
            entries_rejected_confirmation = self.stats.entries_rejected_confirmation,
            entries_rejected_no_price = self.stats.entries_rejected_no_price,
            open_rate,
            balance = self.balance,
            equity = self.equity,
            used_margin = self.used_margin,
            free_margin = self.free_margin,
            "stats"
        );
    }

    fn process_pending_entries(&mut self, symbol: &str, snapshot: MarketSnapshot) {
        let mut due_entries = Vec::new();
        let mut index = 0;
        while index < self.pending_entries.len() {
            if self.pending_entries[index].symbol == symbol
                && self.pending_entries[index].scheduled_entry_time_ms <= snapshot.time_ms
            {
                due_entries.push(self.pending_entries.swap_remove(index));
            } else {
                index += 1;
            }
        }

        for pending in due_entries {
            self.resolve_pending_entry(pending, snapshot);
        }
    }

    fn resolve_pending_entry(&mut self, pending: PendingEntry, snapshot: MarketSnapshot) {
        if snapshot.price <= 0.0 {
            self.stats.entries_rejected_no_price += 1;
            info!(
                target: "paper",
                signal_event_id = %pending.signal_event_id,
                symbol = %pending.symbol,
                direction = pending.direction.as_str(),
                signal_time_ms = pending.signal_time_ms,
                signal_reference_price = pending.signal_reference_price,
                current_price = snapshot.price,
                rejection_reason = "no_entry_price",
                confirmation_mode = confirmation_mode_str(self.config.entry_confirmation_mode),
                "pending entry rejected"
            );
            return;
        }

        let move_bps = directional_move_bps(
            pending.direction,
            pending.signal_reference_price,
            snapshot.price,
        );
        let adverse_bps = adverse_move_bps(
            pending.direction,
            pending.signal_reference_price,
            snapshot.price,
        );

        if let Some(rejection_reason) = self.confirmation_rejection_reason(move_bps, adverse_bps) {
            self.stats.entries_rejected_confirmation += 1;
            info!(
                target: "paper",
                signal_event_id = %pending.signal_event_id,
                symbol = %pending.symbol,
                direction = pending.direction.as_str(),
                signal_time_ms = pending.signal_time_ms,
                signal_reference_price = pending.signal_reference_price,
                current_price = snapshot.price,
                move_bps,
                adverse_bps,
                rejection_reason,
                confirmation_mode = confirmation_mode_str(self.config.entry_confirmation_mode),
                "pending entry rejected"
            );
            return;
        }

        if self.active_positions.len() >= self.config.max_concurrent_positions {
            self.stats.entries_rejected_confirmation += 1;
            info!(
                target: "paper",
                signal_event_id = %pending.signal_event_id,
                symbol = %pending.symbol,
                direction = pending.direction.as_str(),
                signal_time_ms = pending.signal_time_ms,
                signal_reference_price = pending.signal_reference_price,
                current_price = snapshot.price,
                move_bps,
                adverse_bps,
                rejection_reason = "max_concurrent_positions",
                confirmation_mode = confirmation_mode_str(self.config.entry_confirmation_mode),
                "pending entry rejected"
            );
            return;
        }
        if self.config.one_position_per_symbol
            && self
                .active_positions
                .iter()
                .any(|position| position.symbol == pending.symbol)
        {
            self.stats.entries_rejected_confirmation += 1;
            info!(
                target: "paper",
                signal_event_id = %pending.signal_event_id,
                symbol = %pending.symbol,
                direction = pending.direction.as_str(),
                signal_time_ms = pending.signal_time_ms,
                signal_reference_price = pending.signal_reference_price,
                current_price = snapshot.price,
                move_bps,
                adverse_bps,
                rejection_reason = "symbol_already_has_active_position",
                confirmation_mode = confirmation_mode_str(self.config.entry_confirmation_mode),
                "pending entry rejected"
            );
            return;
        }

        let leverage = self.config.leverage.max(1.0);
        let notional_value = self.config.position_size * snapshot.price;
        let margin_used = if leverage > 0.0 {
            notional_value / leverage
        } else {
            0.0
        };
        if margin_used <= 0.0 {
            self.stats.entries_rejected_confirmation += 1;
            info!(
                target: "paper",
                signal_event_id = %pending.signal_event_id,
                symbol = %pending.symbol,
                direction = pending.direction.as_str(),
                signal_time_ms = pending.signal_time_ms,
                signal_reference_price = pending.signal_reference_price,
                current_price = snapshot.price,
                move_bps,
                adverse_bps,
                rejection_reason = "invalid_margin_requirement",
                confirmation_mode = confirmation_mode_str(self.config.entry_confirmation_mode),
                "pending entry rejected"
            );
            return;
        }

        self.refresh_account_metrics();
        if self.free_margin < margin_used {
            self.stats.entries_rejected_confirmation += 1;
            info!(
                target: "paper",
                signal_event_id = %pending.signal_event_id,
                symbol = %pending.symbol,
                direction = pending.direction.as_str(),
                signal_time_ms = pending.signal_time_ms,
                signal_reference_price = pending.signal_reference_price,
                current_price = snapshot.price,
                move_bps,
                adverse_bps,
                rejection_reason = "insufficient_free_margin",
                confirmation_mode = confirmation_mode_str(self.config.entry_confirmation_mode),
                "pending entry rejected"
            );
            return;
        }

        let liquidation_price = compute_liquidation_price(
            pending.direction,
            snapshot.price,
            leverage,
            self.config.maintenance_margin_ratio,
        );
        let position = PaperPosition {
            id: pending.id,
            symbol: pending.symbol.clone(),
            direction: pending.direction,
            entry_price: snapshot.price,
            entry_time_ms: snapshot.time_ms,
            size: self.config.position_size,
            leverage,
            notional_value,
            margin_used,
            liquidation_price,
            take_profit_bps: positive_bps(self.config.take_profit_bps),
            stop_loss_bps: positive_bps(self.config.stop_loss_bps),
            max_duration_ms: (self.config.max_duration_s as i64) * 1000,
            exit_price: None,
            exit_time_ms: None,
            exit_reason: None,
            realized_pnl: 0.0,
            max_unrealized_pnl: 0.0,
            min_unrealized_pnl: 0.0,
        };

        info!(
            target: "paper",
            signal_event_id = %pending.signal_event_id,
            position_id = %position.id,
            symbol = %position.symbol,
            direction = position.direction.as_str(),
            entry_price = position.entry_price,
            signal_reference_price = pending.signal_reference_price,
            size = position.size,
            move_bps,
            adverse_bps,
            confirmation_mode = confirmation_mode_str(self.config.entry_confirmation_mode),
            leverage = position.leverage,
            margin_used = position.margin_used,
            notional_value = position.notional_value,
            liquidation_price = position.liquidation_price,
            "position opened"
        );

        self.stats.total_positions += 1;
        self.stats.entries_opened += 1;
        self.used_margin += position.margin_used;
        self.active_positions.push(position);
        self.refresh_account_metrics();
    }

    fn confirmation_rejection_reason(
        &self,
        move_bps: f64,
        adverse_bps: f64,
    ) -> Option<&'static str> {
        let min_move = self.config.entry_confirmation_return_bps;
        let max_adverse = self.config.entry_confirmation_max_adverse_bps;
        match self.config.entry_confirmation_mode {
            PaperEntryConfirmationMode::None => None,
            PaperEntryConfirmationMode::Directional => {
                if adverse_bps > max_adverse {
                    Some("adverse_move_exceeded")
                } else if move_bps < min_move {
                    Some("directional_confirmation_failed")
                } else {
                    None
                }
            }
            PaperEntryConfirmationMode::Pullback => {
                if adverse_bps > max_adverse {
                    Some("adverse_move_exceeded")
                } else if move_bps < -min_move {
                    Some("pullback_confirmation_failed")
                } else {
                    None
                }
            }
        }
    }

    fn update_positions_for_symbol(&mut self, symbol: &str, snapshot: MarketSnapshot) {
        let mut closed = Vec::new();

        for (index, position) in self.active_positions.iter_mut().enumerate() {
            if position.symbol != symbol {
                continue;
            }

            let raw_return = pnl_fraction(position.direction, position.entry_price, snapshot.price);
            let leveraged_return = raw_return * position.leverage;
            let pnl = position.margin_used * leveraged_return;
            position.max_unrealized_pnl = position.max_unrealized_pnl.max(pnl);
            position.min_unrealized_pnl = position.min_unrealized_pnl.min(pnl);

            let pnl_bps = leveraged_return * 10_000.0;
            let duration_ms = snapshot.time_ms - position.entry_time_ms;
            let exit_reason = if liquidation_triggered(position, snapshot.price) {
                Some(ExitReason::Liquidation)
            } else if position
                .take_profit_bps
                .map(|threshold| pnl_bps >= threshold)
                .unwrap_or(false)
            {
                Some(ExitReason::TakeProfit)
            } else if position
                .stop_loss_bps
                .map(|threshold| pnl_bps <= -threshold)
                .unwrap_or(false)
            {
                Some(ExitReason::StopLoss)
            } else if position.max_duration_ms > 0 && duration_ms >= position.max_duration_ms {
                Some(ExitReason::Timeout)
            } else {
                None
            };

            if let Some(reason) = exit_reason {
                let realized_pnl = if matches!(reason, ExitReason::Liquidation) {
                    -position.margin_used
                } else {
                    pnl
                };
                closed.push((
                    index,
                    snapshot.price,
                    snapshot.time_ms,
                    reason,
                    leveraged_return,
                    realized_pnl,
                ));
            }
        }

        for (index, exit_price, exit_time_ms, exit_reason, leveraged_return, realized_pnl) in
            closed.into_iter().rev()
        {
            let mut position = self.active_positions.swap_remove(index);
            position.exit_price = Some(exit_price);
            position.exit_time_ms = Some(exit_time_ms);
            position.exit_reason = Some(exit_reason);
            position.realized_pnl = realized_pnl;
            self.record_closed_position(position, leveraged_return);
        }
    }

    fn record_closed_position(&mut self, position: PaperPosition, leveraged_return: f64) {
        let exit_price = position.exit_price.unwrap_or(position.entry_price);
        let exit_time_ms = position.exit_time_ms.unwrap_or(position.entry_time_ms);
        let exit_reason = position
            .exit_reason
            .map(ExitReason::as_str)
            .unwrap_or("manual")
            .to_string();
        let duration_ms = exit_time_ms - position.entry_time_ms;
        let is_liquidation = position
            .exit_reason
            .map(|reason| matches!(reason, ExitReason::Liquidation))
            .unwrap_or(false);

        self.used_margin = (self.used_margin - position.margin_used).max(0.0);
        self.balance += position.realized_pnl;
        self.refresh_account_metrics();

        if is_liquidation {
            warn!(
                target: "paper",
                id = %position.id,
                symbol = %position.symbol,
                direction = position.direction.as_str(),
                entry_price = position.entry_price,
                liquidation_price = position.liquidation_price,
                loss = position.realized_pnl,
                "position liquidated"
            );
        }

        info!(
            target: "paper",
            id = %position.id,
            symbol = %position.symbol,
            direction = position.direction.as_str(),
            entry_price = position.entry_price,
            exit_price,
            pnl = position.realized_pnl,
            pnl_pct = leveraged_return,
            duration_ms,
            exit_reason = %exit_reason,
            balance_after = self.balance,
            "position closed"
        );

        self.stats.closed_positions += 1;
        self.stats.total_pnl += position.realized_pnl;
        self.stats.total_duration_ms += duration_ms;
        if position.realized_pnl >= 0.0 {
            self.stats.wins += 1;
            self.stats.win_count += 1;
            self.stats.total_win_pnl += position.realized_pnl;
        } else {
            self.stats.losses += 1;
            self.stats.loss_count += 1;
            self.stats.total_loss_pnl += position.realized_pnl;
        }

        let record = PaperPositionRecord {
            id: position.id,
            symbol: position.symbol,
            direction: position.direction,
            entry_price: position.entry_price,
            exit_price,
            pnl: position.realized_pnl,
            duration_ms,
            exit_reason,
            entry_time_ms: position.entry_time_ms,
            exit_time_ms,
        };
        if let Err(err) = self.persistence.send_paper_position(record) {
            warn!(target: "paper", error = %err, "failed to persist paper position");
        }
    }

    fn refresh_account_metrics(&mut self) {
        let unrealized_pnl = self
            .active_positions
            .iter()
            .filter_map(|position| {
                self.latest_market.get(&position.symbol).map(|snapshot| {
                    let raw_return =
                        pnl_fraction(position.direction, position.entry_price, snapshot.price);
                    position.margin_used * raw_return * position.leverage
                })
            })
            .sum::<f64>();
        self.equity = self.balance + unrealized_pnl;
        self.free_margin = self.equity - self.used_margin;
    }
}

fn pnl_fraction(direction: Direction, entry_price: f64, current_price: f64) -> f64 {
    if entry_price <= 0.0 {
        return 0.0;
    }
    match direction {
        Direction::Long => (current_price - entry_price) / entry_price,
        Direction::Short => (entry_price - current_price) / entry_price,
    }
}

fn positive_bps(value: f64) -> Option<f64> {
    if value > 0.0 { Some(value) } else { None }
}

fn compute_liquidation_price(
    direction: Direction,
    entry_price: f64,
    leverage: f64,
    maintenance_margin_ratio: f64,
) -> Option<f64> {
    if entry_price <= 0.0 || leverage <= 0.0 {
        return None;
    }
    let liquidation_threshold = (1.0 / leverage) - maintenance_margin_ratio;
    if liquidation_threshold <= 0.0 {
        return None;
    }
    Some(match direction {
        Direction::Long => entry_price * (1.0 - liquidation_threshold),
        Direction::Short => entry_price * (1.0 + liquidation_threshold),
    })
}

fn liquidation_triggered(position: &PaperPosition, current_price: f64) -> bool {
    match position.liquidation_price {
        Some(liquidation_price) => match position.direction {
            Direction::Long => current_price <= liquidation_price,
            Direction::Short => current_price >= liquidation_price,
        },
        None => false,
    }
}

fn directional_move_bps(direction: Direction, reference_price: f64, current_price: f64) -> f64 {
    if reference_price <= 0.0 {
        return 0.0;
    }
    let raw_bps = ((current_price - reference_price) / reference_price) * 10_000.0;
    match direction {
        Direction::Long => raw_bps,
        Direction::Short => -raw_bps,
    }
}

fn adverse_move_bps(direction: Direction, reference_price: f64, current_price: f64) -> f64 {
    (-directional_move_bps(direction, reference_price, current_price)).max(0.0)
}

fn confirmation_mode_str(mode: PaperEntryConfirmationMode) -> &'static str {
    match mode {
        PaperEntryConfirmationMode::None => "none",
        PaperEntryConfirmationMode::Directional => "directional",
        PaperEntryConfirmationMode::Pullback => "pullback",
    }
}
