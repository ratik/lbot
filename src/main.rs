mod config;
mod evaluation;
mod features;
mod paper_trading;
mod persistence;
mod runtime;
mod scoring;
mod signals;
mod sources;
mod state;
mod types;

use anyhow::Result;
use rustls::crypto::ring::default_provider;
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::AppConfig;

#[tokio::main]
async fn main() -> Result<()> {
    default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls ring crypto provider"))?;

    let config = AppConfig::load()?;

    let filter = EnvFilter::try_new(config.log_level.clone())
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_names(false)
        .compact()
        .init();

    runtime::run(config).await
}
