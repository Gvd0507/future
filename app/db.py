from __future__ import annotations

from pathlib import Path
import duckdb


DDL = """
CREATE TABLE IF NOT EXISTS candles (
    symbol VARCHAR NOT NULL,
    interval VARCHAR NOT NULL,
    event_ts TIMESTAMP NOT NULL,
    close_ts TIMESTAMP NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    quote_volume DOUBLE,
    trade_count BIGINT,
    taker_buy_base DOUBLE,
    taker_buy_quote DOUBLE,
    ingestion_ts TIMESTAMP NOT NULL,
    PRIMARY KEY(symbol, interval, event_ts)
);

CREATE TABLE IF NOT EXISTS open_interest (
    symbol VARCHAR NOT NULL,
    interval VARCHAR NOT NULL,
    event_ts TIMESTAMP NOT NULL,
    open_interest DOUBLE NOT NULL,
    open_interest_value DOUBLE,
    ingestion_ts TIMESTAMP NOT NULL,
    PRIMARY KEY(symbol, interval, event_ts)
);

CREATE TABLE IF NOT EXISTS funding_rates (
    symbol VARCHAR NOT NULL,
    event_ts TIMESTAMP NOT NULL,
    funding_rate DOUBLE NOT NULL,
    mark_price DOUBLE,
    ingestion_ts TIMESTAMP NOT NULL,
    PRIMARY KEY(symbol, event_ts)
);

CREATE TABLE IF NOT EXISTS divergence_signals (
    signal_id VARCHAR PRIMARY KEY,
    symbol VARCHAR NOT NULL,
    signal_ts TIMESTAMP NOT NULL,
    window_start_ts TIMESTAMP NOT NULL,
    divergence_type VARCHAR NOT NULL,
    beta_4h DOUBLE NOT NULL,
    baseline_beta DOUBLE NOT NULL,
    corr_4h DOUBLE,
    corr_24h DOUBLE,
    corr_7d DOUBLE,
    oi_delta_zscore DOUBLE NOT NULL,
    alt_oi_change DOUBLE NOT NULL,
    btc_oi_change DOUBLE NOT NULL,
    funding_rate DOUBLE,
    btc_regime VARCHAR NOT NULL,
    signal_score DOUBLE,
    ingestion_ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS simulated_trades (
    trade_id VARCHAR PRIMARY KEY,
    signal_id VARCHAR,
    symbol VARCHAR NOT NULL,
    signal_ts TIMESTAMP NOT NULL,
    entry_ts TIMESTAMP NOT NULL,
    entry_price DOUBLE NOT NULL,
    size_usd DOUBLE NOT NULL,
    units DOUBLE NOT NULL,
    stop_price DOUBLE NOT NULL,
    take_profit_price DOUBLE NOT NULL,
    slippage_bps DOUBLE NOT NULL,
    funding_cost_usd DOUBLE NOT NULL,
    pnl_4h DOUBLE,
    pnl_12h DOUBLE,
    pnl_24h DOUBLE,
    win_4h BOOLEAN,
    win_12h BOOLEAN,
    win_24h BOOLEAN,
    baseline_flag BOOLEAN NOT NULL,
    btc_regime VARCHAR,
    funding_env VARCHAR,
    distribution_flag BOOLEAN,
    entry_delay_candles INTEGER,
    corr7d_pre_signal DOUBLE,
    corr7d_drop_type VARCHAR,
    ingestion_ts TIMESTAMP NOT NULL
);
"""


def ensure_data_dir(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)


def get_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    ensure_data_dir(db_path)
    conn = duckdb.connect(db_path)
    conn.execute("PRAGMA threads=4;")
    conn.execute(DDL)
    return conn
