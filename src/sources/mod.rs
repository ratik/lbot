pub mod binance;
pub mod bybit;
pub mod dydx;
pub mod okx;

use anyhow::Result;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{config::AppConfig, types::MarketEvent};

pub type SourceTx = mpsc::Sender<MarketEvent>;

pub fn spawn_sources(config: &AppConfig, tx: SourceTx) -> Result<Vec<JoinHandle<()>>> {
    let mut handles = Vec::new();
    if config.venues.binance.enabled {
        handles.push(binance::spawn(config, tx.clone()));
    }
    if config.venues.bybit.enabled {
        handles.push(bybit::spawn(config, tx.clone()));
    }
    if config.venues.okx.enabled {
        handles.push(okx::spawn(config, tx.clone()));
    }
    if config.venues.dydx.enabled {
        handles.push(dydx::spawn(config, tx));
    }
    Ok(handles)
}
