use std::collections::HashMap;

use crate::types::{Direction, Venue};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalScope {
    Venue { venue: Venue },
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SignalKey {
    pub scope: SignalScope,
    pub direction: Direction,
}

#[derive(Debug, Clone)]
pub struct SignalGate {
    threshold: f64,
    rearm_level: f64,
    cooldown_ms: i64,
    armed: bool,
    last_fired_ms: Option<i64>,
}

impl SignalGate {
    pub fn new(threshold: f64, rearm_ratio: f64, cooldown_ms: i64) -> Self {
        Self {
            threshold,
            rearm_level: threshold * rearm_ratio,
            cooldown_ms,
            armed: true,
            last_fired_ms: None,
        }
    }

    pub fn should_fire(&mut self, score: f64, now_ms: i64) -> bool {
        if !self.armed && score < self.rearm_level {
            self.armed = true;
        }
        if !self.armed || score < self.threshold {
            return false;
        }
        if let Some(last_fired_ms) = self.last_fired_ms {
            if now_ms - last_fired_ms < self.cooldown_ms {
                return false;
            }
        }
        self.armed = false;
        self.last_fired_ms = Some(now_ms);
        true
    }
}

#[derive(Debug, Default)]
pub struct SignalRegistry {
    gates: HashMap<(String, SignalKey), SignalGate>,
}

impl SignalRegistry {
    pub fn gate_mut(
        &mut self,
        symbol: &str,
        key: SignalKey,
        threshold: f64,
        rearm_ratio: f64,
        cooldown_ms: i64,
    ) -> &mut SignalGate {
        self.gates
            .entry((symbol.to_string(), key))
            .or_insert_with(|| SignalGate::new(threshold, rearm_ratio, cooldown_ms))
    }
}
