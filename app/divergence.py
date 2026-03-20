from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
import uuid

import numpy as np
import pandas as pd

from app.config import settings
from app.db import get_connection
from app.indicators import adx, compute_log_returns, confirmed_swing_lows, rolling_beta, rolling_corr


def _load_candles(conn, symbol: str, interval: str) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT symbol, interval, event_ts, open, high, low, close, volume
        FROM candles
        WHERE symbol = ? AND interval = ?
        ORDER BY event_ts
        """,
        [symbol, interval],
    ).df()


def _load_open_interest(conn, symbol: str) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT symbol, event_ts, open_interest
        FROM open_interest
        WHERE symbol = ? AND interval = '5m'
        ORDER BY event_ts
        """,
        [symbol],
    ).df()


def _load_funding(conn, symbol: str) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT symbol, event_ts, funding_rate
        FROM funding_rates
        WHERE symbol = ?
        ORDER BY event_ts
        """,
        [symbol],
    ).df()


def _classify_btc_regime(conn) -> pd.DataFrame:
    btc_1h = _load_candles(conn, settings.btc_symbol, "1h")
    if btc_1h.empty:
        return pd.DataFrame(columns=["event_ts", "btc_regime", "adx"])
    btc_1h["adx"] = adx(btc_1h, settings.adx_period)
    btc_1h["btc_regime"] = np.where(
        btc_1h["adx"] > settings.adx_trending_threshold,
        "trending",
        "ranging",
    )
    return btc_1h[["event_ts", "btc_regime", "adx"]]


@dataclass
class SignalRecord:
    signal_id: str
    symbol: str
    signal_ts: pd.Timestamp
    window_start_ts: pd.Timestamp
    divergence_type: str
    beta_4h: float
    baseline_beta: float
    corr_4h: float
    corr_24h: float
    corr_7d: float
    oi_delta_zscore: float
    alt_oi_change: float
    btc_oi_change: float
    funding_rate: float | None
    btc_regime: str
    signal_score: float


def detect_divergence_signals() -> pd.DataFrame:
    conn = get_connection(settings.db_path)

    btc_5m = _load_candles(conn, settings.btc_symbol, "5m")
    btc_5m["ret"] = compute_log_returns(btc_5m["close"])
    btc_swings = confirmed_swing_lows(btc_5m, settings.swing_confirmation_candles)

    btc_oi = _load_open_interest(conn, settings.btc_symbol)
    btc_oi["btc_oi_change"] = btc_oi["open_interest"].diff()

    regime = _classify_btc_regime(conn)
    signals: list[SignalRecord] = []

    for alt in settings.alt_symbols:
        alt_5m = _load_candles(conn, alt, "5m")
        if alt_5m.empty or btc_5m.empty:
            continue

        alt_5m["ret"] = compute_log_returns(alt_5m["close"])
        merged = btc_5m[["event_ts", "close", "ret", "low"]].merge(
            alt_5m[["event_ts", "close", "ret", "low"]],
            on="event_ts",
            suffixes=("_btc", "_alt"),
            how="inner",
        )
        if merged.empty:
            continue

        merged["beta_4h"] = rolling_beta(
            merged["ret_alt"], merged["ret_btc"], settings.correlation_windows_bars_5m["4h"]
        )
        merged["corr_4h"] = rolling_corr(
            merged["ret_alt"], merged["ret_btc"], settings.correlation_windows_bars_5m["4h"]
        )
        merged["corr_24h"] = rolling_corr(
            merged["ret_alt"], merged["ret_btc"], settings.correlation_windows_bars_5m["24h"]
        )
        merged["corr_7d"] = rolling_corr(
            merged["ret_alt"], merged["ret_btc"], settings.correlation_windows_bars_5m["7d"]
        )
        merged["baseline_beta"] = merged["beta_4h"].rolling(settings.correlation_windows_bars_5m["24h"]).mean()

        alt_swings = confirmed_swing_lows(alt_5m, settings.swing_confirmation_candles)
        alt_oi = _load_open_interest(conn, alt)
        alt_oi["alt_oi_change"] = alt_oi["open_interest"].diff()
        alt_oi["oi_std_30d"] = alt_oi["alt_oi_change"].rolling(30 * 24 * 12).std()
        alt_oi["oi_delta_zscore"] = alt_oi["alt_oi_change"] / alt_oi["oi_std_30d"].replace(0, np.nan)

        funding = _load_funding(conn, alt)

        alt_swing_lookup = alt_swings.set_index("confirm_ts") if not alt_swings.empty else pd.DataFrame()

        for _, btc_s in btc_swings.iterrows():
            signal_ts = pd.Timestamp(btc_s["confirm_ts"]).tz_localize(timezone.utc) if pd.Timestamp(btc_s["confirm_ts"]).tzinfo is None else pd.Timestamp(btc_s["confirm_ts"])
            row = merged[merged["event_ts"] == signal_ts]
            if row.empty:
                continue

            row = row.iloc[0]
            if not bool(btc_s["lower_than_previous_swing"]):
                continue
            if pd.isna(row["beta_4h"]) or pd.isna(row["baseline_beta"]):
                continue
            if row["beta_4h"] >= 0.3 or row["baseline_beta"] <= 1.0:
                continue

            alt_match = alt_swing_lookup.loc[[signal_ts]] if not alt_swings.empty and signal_ts in alt_swing_lookup.index else pd.DataFrame()
            alt_non_confirmation = True
            if not alt_match.empty:
                if isinstance(alt_match, pd.Series):
                    alt_prior_low = alt_match.get("prior_swing_low", np.nan)
                    alt_pivot_low = alt_match.get("pivot_low", np.nan)
                else:
                    alt_prior_low = alt_match.iloc[-1].get("prior_swing_low", np.nan)
                    alt_pivot_low = alt_match.iloc[-1].get("pivot_low", np.nan)
                alt_non_confirmation = pd.isna(alt_prior_low) or (alt_pivot_low >= alt_prior_low)

            if not alt_non_confirmation:
                continue

            oi_alt_row = alt_oi[alt_oi["event_ts"] == signal_ts]
            oi_btc_row = btc_oi[btc_oi["event_ts"] == signal_ts]
            if oi_alt_row.empty or oi_btc_row.empty:
                continue

            oi_alt_row = oi_alt_row.iloc[0]
            oi_btc_row = oi_btc_row.iloc[0]
            if pd.isna(oi_alt_row["oi_delta_zscore"]) or oi_alt_row["oi_delta_zscore"] <= 1.0:
                continue
            if pd.isna(oi_btc_row["btc_oi_change"]) or oi_btc_row["btc_oi_change"] > 0:
                continue

            funding_row = funding[funding["event_ts"] <= signal_ts].tail(1)
            funding_rate = float(funding_row.iloc[0]["funding_rate"]) if not funding_row.empty else None

            regime_row = regime[regime["event_ts"] <= signal_ts].tail(1)
            btc_regime = regime_row.iloc[0]["btc_regime"] if not regime_row.empty else "unknown"

            signal_score = (
                abs(0.3 - row["beta_4h"]) * 2.0
                + abs(float(oi_alt_row["oi_delta_zscore"]))
                + max(0.0, 1.0 - (row["corr_4h"] if not pd.isna(row["corr_4h"]) else 0.0))
            )

            signals.append(
                SignalRecord(
                    signal_id=str(uuid.uuid4()),
                    symbol=alt,
                    signal_ts=signal_ts,
                    window_start_ts=signal_ts - pd.Timedelta(hours=4),
                    divergence_type="btc_down_alt_holds",
                    beta_4h=float(row["beta_4h"]),
                    baseline_beta=float(row["baseline_beta"]),
                    corr_4h=float(row["corr_4h"]) if not pd.isna(row["corr_4h"]) else np.nan,
                    corr_24h=float(row["corr_24h"]) if not pd.isna(row["corr_24h"]) else np.nan,
                    corr_7d=float(row["corr_7d"]) if not pd.isna(row["corr_7d"]) else np.nan,
                    oi_delta_zscore=float(oi_alt_row["oi_delta_zscore"]),
                    alt_oi_change=float(oi_alt_row["alt_oi_change"]),
                    btc_oi_change=float(oi_btc_row["btc_oi_change"]),
                    funding_rate=funding_rate,
                    btc_regime=btc_regime,
                    signal_score=float(signal_score),
                )
            )

    signals_df = pd.DataFrame([s.__dict__ for s in signals])
    if not signals_df.empty:
        signals_df["ingestion_ts"] = pd.Timestamp.utcnow()
        conn.register("signals_df", signals_df)
        conn.execute("INSERT OR REPLACE INTO divergence_signals SELECT * FROM signals_df")
        conn.unregister("signals_df")

    conn.close()
    return signals_df
