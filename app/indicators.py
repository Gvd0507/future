from __future__ import annotations

import numpy as np
import pandas as pd


def compute_log_returns(close: pd.Series) -> pd.Series:
    return np.log(close).diff()


def rolling_beta(x: pd.Series, y: pd.Series, window: int) -> pd.Series:
    cov = x.rolling(window).cov(y)
    var = y.rolling(window).var()
    return cov / var.replace(0, np.nan)


def rolling_corr(x: pd.Series, y: pd.Series, window: int) -> pd.Series:
    return x.rolling(window).corr(y)


def adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]

    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr.replace(0, np.nan))
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr.replace(0, np.nan))
    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)).fillna(0)
    return dx.rolling(period).mean()


def confirmed_swing_lows(df: pd.DataFrame, hold_candles: int = 3) -> pd.DataFrame:
    lows = df["low"].to_numpy()
    ts = df["event_ts"].to_numpy()

    swing_rows = []
    prev_swing_low = np.inf

    for i in range(1, len(df) - hold_candles):
        current_low = lows[i]
        if current_low >= lows[i - 1]:
            continue

        future_lows = lows[i + 1 : i + 1 + hold_candles]
        if np.any(future_lows <= current_low):
            continue

        lower_low = current_low < prev_swing_low
        confirmation_idx = i + hold_candles
        swing_rows.append(
            {
                "pivot_idx": i,
                "pivot_ts": ts[i],
                "pivot_low": current_low,
                "confirm_idx": confirmation_idx,
                "confirm_ts": ts[confirmation_idx],
                "lower_than_previous_swing": lower_low,
                "prior_swing_low": prev_swing_low if np.isfinite(prev_swing_low) else np.nan,
            }
        )

        if np.isfinite(prev_swing_low):
            prev_swing_low = min(prev_swing_low, current_low)
        else:
            prev_swing_low = current_low

    return pd.DataFrame(swing_rows)
