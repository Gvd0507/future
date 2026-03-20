from __future__ import annotations

import numpy as np
import pandas as pd

from app.config import settings
from app.db import get_connection
from app.simulation import bootstrap_baseline_pvalues


def load_latest_signals() -> pd.DataFrame:
    conn = get_connection(settings.db_path)
    df = conn.execute(
        """
        SELECT *
        FROM divergence_signals
        ORDER BY signal_ts DESC
        """
    ).df()
    conn.close()
    return df


def load_latest_trades() -> pd.DataFrame:
    conn = get_connection(settings.db_path)
    df = conn.execute(
        """
        SELECT *
        FROM simulated_trades
        ORDER BY entry_ts DESC
        """
    ).df()
    conn.close()
    return df


def live_correlation_heatmap_data() -> pd.DataFrame:
    conn = get_connection(settings.db_path)
    query = """
    WITH base AS (
        SELECT symbol, event_ts, close,
               LN(close) - LN(LAG(close) OVER(PARTITION BY symbol ORDER BY event_ts)) AS ret
        FROM candles
        WHERE interval='5m' AND symbol IN ('BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','AVAXUSDT','MATICUSDT','ARBUSDT')
    ),
    btc AS (
        SELECT event_ts, ret AS btc_ret
        FROM base
        WHERE symbol='BTCUSDT'
    ),
    merged AS (
        SELECT b.symbol, b.event_ts, b.ret AS alt_ret, btc.btc_ret
        FROM base b
        JOIN btc USING(event_ts)
        WHERE b.symbol <> 'BTCUSDT'
    )
    SELECT * FROM merged ORDER BY event_ts
    """
    merged = conn.execute(query).df()
    conn.close()

    if merged.empty:
        return pd.DataFrame()

    out = []
    windows = {"4h": 48, "24h": 288, "7d": 2016}
    for symbol, g in merged.groupby("symbol"):
        g = g.sort_values("event_ts")
        for label, w in windows.items():
            corr = g["alt_ret"].rolling(w).corr(g["btc_ret"]).iloc[-1]
            out.append({"symbol": symbol, "window": label, "correlation": corr})
    return pd.DataFrame(out)


def simulation_breakdowns() -> dict[str, pd.DataFrame | list[dict]]:
    trades = load_latest_trades()
    if trades.empty:
        return {
            "by_coin": pd.DataFrame(),
            "by_regime": pd.DataFrame(),
            "by_funding": pd.DataFrame(),
            "baseline": [],
        }

    signal = trades[~trades["baseline_flag"]]

    def _agg(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        return (
            df.groupby(group_cols, dropna=False)
            .agg(
                count=("trade_id", "count"),
                win_rate_4h=("win_4h", "mean"),
                win_rate_12h=("win_12h", "mean"),
                win_rate_24h=("win_24h", "mean"),
                avg_pnl_4h=("pnl_4h", "mean"),
                avg_pnl_12h=("pnl_12h", "mean"),
                avg_pnl_24h=("pnl_24h", "mean"),
            )
            .reset_index()
        )

    by_coin = _agg(signal, ["symbol"])
    by_regime = _agg(signal, ["btc_regime"])
    by_funding = _agg(signal, ["funding_env"])

    baseline_stats = [
        bootstrap_baseline_pvalues(trades, "4h"),
        bootstrap_baseline_pvalues(trades, "12h"),
        bootstrap_baseline_pvalues(trades, "24h"),
    ]

    return {
        "by_coin": by_coin,
        "by_regime": by_regime,
        "by_funding": by_funding,
        "baseline": baseline_stats,
    }


def where_this_breaks() -> dict[str, pd.DataFrame]:
    trades = load_latest_trades()
    signal = trades[~trades["baseline_flag"]].copy()
    if signal.empty:
        empty = pd.DataFrame()
        return {
            "worst_signals": empty,
            "delay_decay": empty,
            "corr_regime": empty,
            "distribution_cases": empty,
        }

    worst = signal.nsmallest(5, "pnl_24h")[[
        "trade_id",
        "symbol",
        "entry_ts",
        "pnl_24h",
        "funding_env",
        "btc_regime",
        "slippage_bps",
        "corr7d_drop_type",
    ]]

    delay_decay = (
        signal.groupby("entry_delay_candles")
        .agg(win_rate_24h=("win_24h", "mean"), avg_pnl_24h=("pnl_24h", "mean"), count=("trade_id", "count"))
        .reset_index()
        .sort_values("entry_delay_candles")
    )

    corr_regime = (
        signal.groupby("corr7d_drop_type")
        .agg(win_rate_24h=("win_24h", "mean"), avg_pnl_24h=("pnl_24h", "mean"), count=("trade_id", "count"))
        .reset_index()
    )

    distribution_cases = signal[signal["distribution_flag"]].copy()
    if not distribution_cases.empty:
        distribution_cases = distribution_cases[[
            "trade_id",
            "symbol",
            "entry_ts",
            "pnl_24h",
            "funding_env",
            "btc_regime",
            "distribution_flag",
        ]]

    return {
        "worst_signals": worst,
        "delay_decay": delay_decay,
        "corr_regime": corr_regime,
        "distribution_cases": distribution_cases,
    }


def findings_snapshot() -> dict:
    breakdowns = simulation_breakdowns()
    breaks = where_this_breaks()

    by_regime = breakdowns["by_regime"]
    by_funding = breakdowns["by_funding"]
    baseline = breakdowns["baseline"]

    return {
        "regime": by_regime.to_dict(orient="records") if isinstance(by_regime, pd.DataFrame) else [],
        "funding": by_funding.to_dict(orient="records") if isinstance(by_funding, pd.DataFrame) else [],
        "baseline": baseline,
        "worst_signals": breaks["worst_signals"].to_dict(orient="records"),
        "distribution_case_ratio": float(
            len(breaks["distribution_cases"]) / max(1, len(load_latest_trades()[~load_latest_trades()["baseline_flag"]]))
        ) if isinstance(breaks["distribution_cases"], pd.DataFrame) else np.nan,
    }
