# Liquidation-Risk Watcher

Analytics-only MVP for watching liquidation-risk conditions across multiple crypto derivatives venues in real time. This project does not place orders, does not require account auth, and is designed to log signals plus enough forward outcomes to judge whether the signals were useful.

## What the signals mean

`LONG_LIQ_RISK` means downside stress is building in a regime where crowded longs may be vulnerable to forced unwinds. It is a bearish continuation risk signal.

`SHORT_LIQ_RISK` means upside stress is building in a regime where crowded shorts may be vulnerable to forced unwinds. It is a bullish continuation risk signal.

These are probabilistic stress signals, not deterministic liquidation detectors.

## Signal levels

Venue-level signals:
- `VENUE_LONG_LIQ_RISK`
- `VENUE_SHORT_LIQ_RISK`

These are produced per `(symbol, venue)` from rolling returns, signed trade flow, burstiness, spread stress, and realized volatility. The patched venue signal path uses `250ms` online buckets by default, directional gating on `return_1s` and `signed_volume_1s`, and a warmup period before any venue event is eligible to fire.

Market-level signals:
- `MARKET_LONG_LIQ_RISK`
- `MARKET_SHORT_LIQ_RISK`

These aggregate venue scores across Binance, Bybit, OKX, and optionally dYdX. By default the market-level event requires at least two venues confirming in the same direction.

## Current data sources

MVP venues:
- Binance futures: trades + `bookTicker`
- Bybit linear public WS: public trades + ticker best bid/ask
- OKX public WS: trades + tickers

Optional follower venue:
- dYdX is wired into the architecture but left as a placeholder in the MVP

Future extensions are expected for liquidation feeds, open interest, and funding. The hot path already normalizes venue messages into internal trade and quote tick types so those additions can be layered in without changing the core loop.

## Outcome evaluation

Every emitted signal is evaluated automatically after configurable horizons:
- 10s
- 30s
- 60s
- 120s

For each horizon the service records:
- direction-aware return in bps
- max favorable excursion
- max adverse excursion
- realized volatility after the signal
- success flag using per-symbol continuation thresholds

Defaults:
- BTCUSDT: 20 bps
- ETHUSDT: 25 bps
- SOLUSDT: 35 bps

## Persistence

SQLite is used for operational persistence with WAL mode enabled.

Tables:
- `venue_signals`
- `market_signals`
- `outcome_checks`
- `service_heartbeats`

Database writes are handled on a dedicated worker thread so the online state update path does not block on SQLite.

## Config

Configuration is loaded from `config.toml` by default. Supported env overrides:
- `LBOT_CONFIG`
- `LBOT_SYMBOLS`
- `LBOT_SQLITE_PATH`
- `LBOT_LOG_LEVEL`
- `LBOT_ENABLED_VENUES`

Important patched signal defaults:
- `online_bucket_ms = 250`
- `warmup_seconds = 120`
- `venue_signal_threshold = 5.0`
- `min_directional_core = 1.5`
- `max_stress_amplifier = 3.0`
- `min_return_bps = 1.0`
- `long_return_z_gate = 0.5`
- `long_volume_z_gate = 0.5`
- `short_return_z_gate = 0.5`
- `short_volume_z_gate = 0.5`

## Run locally

```bash
cargo run --release
```

Or with an explicit config path:

```bash
cargo run --release -- config.toml
```

## Current limitations

- No liquidation, funding, or open-interest feeds yet
- Feature normalization is intentionally simple and based on rolling fixed online buckets
- Quote quality is top-of-book only
- dYdX is not implemented yet
- No historical backfill; this is an online watcher

## Design notes

- One Tokio runtime drives all source adapters and the online event loop
- Venue adapters stay small and emit normalized internal events
- Rolling in-memory state is maintained per `(symbol, venue)`
- Scoring and confirmation are computed online from bounded rolling windows
- Outcome checks are delayed tasks evaluated from in-memory blended market price history
