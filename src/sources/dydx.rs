use std::time::Duration;

use tokio::{sync::mpsc, task::JoinHandle};
use tracing::warn;

use crate::{config::AppConfig, types::MarketEvent};

pub fn spawn(_config: &AppConfig, _tx: mpsc::Sender<MarketEvent>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            warn!(
                "dydx source is configured as optional follower venue and is not implemented in the MVP"
            );
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    })
}
