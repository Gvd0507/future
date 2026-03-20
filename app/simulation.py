from __future__ import annotations

from dataclasses import dataclass
import uuid

import numpy as np
import pandas as pd

from app.config import settings
from app.db import get_connection
from app.indicators import confirmed_swing_lows


HORIZON_BARS = {"4h": 48, "12h": 144, "24h": 288}


def _load_5m(conn, symbol: str) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT event_ts, open, high, low, close
        FROM candles
        WHERE symbol = ? AND interval = '5m'
        ORDER BY event_ts
        """,
        [symbol],
    ).df()


def _load_signals(conn) -> pd.DataFrame:
    return conn.execute("SELECT * FROM divergence_signals ORDER BY signal_ts").df()


def _load_funding(conn, symbol: str) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT event_ts, funding_rate
        FROM funding_rates
        WHERE symbol = ?
        ORDER BY event_ts
        """,
        [symbol],
    ).df()


def estimate_empirical_slippage_bps(candles_5m: pd.DataFrame, lookback_bars: int = 30 * 24 * 12) -> float:
    if candles_5m.empty:
        return 8.0
    sample = candles_5m.tail(lookback_bars).copy()
    spread_proxy_bps = ((sample["high"] - sample["low"]) / sample["open"].replace(0, np.nan)) * 10_000
    spread_proxy_bps = spread_proxy_bps.replace([np.inf, -np.inf], np.nan).dropna()
    if spread_proxy_bps.empty:
        return 8.0
    return float(np.clip(np.nanmedian(spread_proxy_bps) * 0.5, 2.0, 25.0))


def _funding_env(rate: float | None) -> str:
    if rate is None or np.isnan(rate):
        return "neutral"
    if rate > 0.00001:
        return "positive"
    if rate < -0.00001:
        return "negative"
    return "neutral"


def _funding_cost_usd(funding_df: pd.DataFrame, entry_ts: pd.Timestamp, exit_ts: pd.Timestamp, notional: float) -> float:
    if funding_df.empty:
        return 0.0
    window = funding_df[(funding_df["event_ts"] > entry_ts) & (funding_df["event_ts"] <= exit_ts)]
    return float((window["funding_rate"] * notional).sum())


def _simulate_trade_path(
    candles: pd.DataFrame,
    entry_idx: int,
    stop_price: float,
    tp_price: float,
    slippage_bps: float,
    notional: float,
) -> dict:
    entry_price_raw = float(candles.iloc[entry_idx]["open"])
    entry_price = entry_price_raw * (1 + slippage_bps / 10_000)
    units = notional / entry_price

    outcomes = {}
    for horizon_name, bars in HORIZON_BARS.items():
        end_idx = min(entry_idx + bars, len(candles) - 1)
        path = candles.iloc[entry_idx : end_idx + 1]
        exit_price = float(path.iloc[-1]["close"]) * (1 - slippage_bps / 10_000)
        hit = None

        for _, row in path.iterrows():
            if row["low"] <= stop_price and row["high"] >= tp_price:
                hit = ("stop", stop_price)
                break
            if row["low"] <= stop_price:
                hit = ("stop", stop_price)
                break
            if row["high"] >= tp_price:
                hit = ("tp", tp_price)
                break

        if hit is not None:
            side, px = hit
            adjusted_px = px * (1 - slippage_bps / 10_000)
            pnl = (adjusted_px - entry_price) * units
            outcomes[horizon_name] = {
                "pnl": pnl,
                "win": bool(side == "tp" or pnl > 0),
                "exit_ts": path.iloc[0]["event_ts"],
            }
        else:
            pnl = (exit_price - entry_price) * units
            outcomes[horizon_name] = {
                "pnl": pnl,
                "win": bool(pnl > 0),
                "exit_ts": path.iloc[-1]["event_ts"],
            }

    return {
        "entry_price": entry_price,
        "units": units,
        "outcomes": outcomes,
    }


@dataclass
class TradeRecord:
    trade_id: str
    signal_id: str | None
    symbol: str
    signal_ts: pd.Timestamp
    entry_ts: pd.Timestamp
    entry_price: float
    size_usd: float
    units: float
    stop_price: float
    take_profit_price: float
    slippage_bps: float
    funding_cost_usd: float
    pnl_4h: float
    pnl_12h: float
    pnl_24h: float
    win_4h: bool
    win_12h: bool
    win_24h: bool
    baseline_flag: bool
    btc_regime: str
    funding_env: str
    distribution_flag: bool
    entry_delay_candles: int
    corr7d_pre_signal: float
    corr7d_drop_type: str


def _btc_swing_move_at_signal(btc_swings: pd.DataFrame, signal_ts: pd.Timestamp) -> float:
    match = btc_swings[btc_swings["confirm_ts"] == signal_ts]
    if match.empty:
        return 0.005
    row = match.iloc[-1]
    prior_low = row.get("prior_swing_low", np.nan)
    pivot_low = row.get("pivot_low", np.nan)
    if pd.isna(prior_low) or pd.isna(pivot_low) or prior_low == 0:
        return 0.005
    return float(abs((pivot_low - prior_low) / prior_low))


def _alt_recent_swing_low(alt_swings: pd.DataFrame, signal_ts: pd.Timestamp) -> float | None:
    prior = alt_swings[alt_swings["confirm_ts"] <= signal_ts].tail(1)
    if prior.empty:
        return None
    return float(prior.iloc[-1]["pivot_low"])


def run_simulation(entry_delay_candles: int = 2, random_seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(random_seed)
    conn = get_connection(settings.db_path)
    signals = _load_signals(conn)
    if signals.empty:
        conn.close()
        return pd.DataFrame()

    btc_5m = _load_5m(conn, settings.btc_symbol)
    btc_swings = confirmed_swing_lows(btc_5m, settings.swing_confirmation_candles)

    records: list[TradeRecord] = []

    for symbol in settings.alt_symbols:
        alt_5m = _load_5m(conn, symbol)
        if alt_5m.empty:
            continue
        alt_swings = confirmed_swing_lows(alt_5m, settings.swing_confirmation_candles)
        funding = _load_funding(conn, symbol)
        slippage_bps = estimate_empirical_slippage_bps(alt_5m)

        symbol_signals = signals[signals["symbol"] == symbol]
        for _, signal in symbol_signals.iterrows():
            signal_ts = signal["signal_ts"]
            idx_match = alt_5m.index[alt_5m["event_ts"] == signal_ts]
            if len(idx_match) == 0:
                continue

            signal_idx = int(idx_match[0])
            entry_idx = signal_idx + entry_delay_candles
            if entry_idx >= len(alt_5m):
                continue

            entry_ts = alt_5m.iloc[entry_idx]["event_ts"]
            recent_swing_low = _alt_recent_swing_low(alt_swings, signal_ts)
            if recent_swing_low is None:
                continue

            stop_price = recent_swing_low * (1 - settings.stop_buffer_pct)
            btc_move = _btc_swing_move_at_signal(btc_swings, signal_ts)
            entry_open = float(alt_5m.iloc[entry_idx]["open"])
            take_profit_price = entry_open * (1 + settings.tp_btc_move_mult * btc_move)

            sim = _simulate_trade_path(
                alt_5m,
                entry_idx=entry_idx,
                stop_price=stop_price,
                tp_price=take_profit_price,
                slippage_bps=slippage_bps,
                notional=settings.notional_usd,
            )

            cost_24h = _funding_cost_usd(
                funding,
                entry_ts=entry_ts,
                exit_ts=entry_ts + pd.Timedelta(hours=24),
                notional=settings.notional_usd,
            )

            pnl_4h = sim["outcomes"]["4h"]["pnl"] - cost_24h
            pnl_12h = sim["outcomes"]["12h"]["pnl"] - cost_24h
            pnl_24h = sim["outcomes"]["24h"]["pnl"] - cost_24h

            distribution_window = alt_5m[(alt_5m["event_ts"] > entry_ts) & (alt_5m["event_ts"] <= entry_ts + pd.Timedelta(minutes=30))]
            moved_against = False
            if not distribution_window.empty:
                moved_against = float(distribution_window["low"].min()) < sim["entry_price"]
            distribution_flag = bool(signal["alt_oi_change"] > 0 and moved_against)

            corr_drop_type = "sudden_drop" if (signal.get("corr_24h", np.nan) - signal.get("corr_7d", np.nan)) < -0.2 else "already_low"

            records.append(
                TradeRecord(
                    trade_id=str(uuid.uuid4()),
                    signal_id=signal["signal_id"],
                    symbol=symbol,
                    signal_ts=signal_ts,
                    entry_ts=entry_ts,
                    entry_price=float(sim["entry_price"]),
                    size_usd=settings.notional_usd,
                    units=float(sim["units"]),
                    stop_price=float(stop_price),
                    take_profit_price=float(take_profit_price),
                    slippage_bps=float(slippage_bps),
                    funding_cost_usd=float(cost_24h),
                    pnl_4h=float(pnl_4h),
                    pnl_12h=float(pnl_12h),
                    pnl_24h=float(pnl_24h),
                    win_4h=bool(pnl_4h > 0),
                    win_12h=bool(pnl_12h > 0),
                    win_24h=bool(pnl_24h > 0),
                    baseline_flag=False,
                    btc_regime=str(signal["btc_regime"]),
                    funding_env=_funding_env(signal.get("funding_rate", np.nan)),
                    distribution_flag=distribution_flag,
                    entry_delay_candles=entry_delay_candles,
                    corr7d_pre_signal=float(signal.get("corr_7d", np.nan)),
                    corr7d_drop_type=corr_drop_type,
                )
            )

    baseline_records = _simulate_baseline(conn, len(records), entry_delay_candles, rng)
    records.extend(baseline_records)

    trades_df = pd.DataFrame([r.__dict__ for r in records])
    if not trades_df.empty:
        trades_df["ingestion_ts"] = pd.Timestamp.utcnow()
        conn.register("trades_df", trades_df)
        conn.execute("INSERT OR REPLACE INTO simulated_trades SELECT * FROM trades_df")
        conn.unregister("trades_df")

    conn.close()
    return trades_df


def _simulate_baseline(conn, n: int, entry_delay_candles: int, rng: np.random.Generator) -> list[TradeRecord]:
    if n <= 0:
        return []

    btc_5m = _load_5m(conn, settings.btc_symbol)
    btc_swings = confirmed_swing_lows(btc_5m, settings.swing_confirmation_candles)
    if btc_swings.empty:
        return []

    records: list[TradeRecord] = []
    chosen_swings = btc_swings.sample(min(n, len(btc_swings)), random_state=42, replace=len(btc_swings) < n)

    for _, swing in chosen_swings.iterrows():
        symbol = settings.alt_symbols[int(rng.integers(0, len(settings.alt_symbols)))]
        alt_5m = _load_5m(conn, symbol)
        if alt_5m.empty:
            continue
        alt_swings = confirmed_swing_lows(alt_5m, settings.swing_confirmation_candles)
        funding = _load_funding(conn, symbol)
        slippage_bps = estimate_empirical_slippage_bps(alt_5m)

        signal_ts = swing["confirm_ts"]
        idx_match = alt_5m.index[alt_5m["event_ts"] == signal_ts]
        if len(idx_match) == 0:
            continue
        signal_idx = int(idx_match[0])
        entry_idx = signal_idx + entry_delay_candles
        if entry_idx >= len(alt_5m):
            continue

        entry_ts = alt_5m.iloc[entry_idx]["event_ts"]
        recent_swing_low = _alt_recent_swing_low(alt_swings, signal_ts)
        if recent_swing_low is None:
            continue

        stop_price = recent_swing_low * (1 - settings.stop_buffer_pct)
        btc_move = _btc_swing_move_at_signal(btc_swings, signal_ts)
        entry_open = float(alt_5m.iloc[entry_idx]["open"])
        take_profit_price = entry_open * (1 + settings.tp_btc_move_mult * btc_move)

        sim = _simulate_trade_path(
            alt_5m,
            entry_idx,
            stop_price,
            take_profit_price,
            slippage_bps,
            settings.notional_usd,
        )
        cost_24h = _funding_cost_usd(
            funding,
            entry_ts=entry_ts,
            exit_ts=entry_ts + pd.Timedelta(hours=24),
            notional=settings.notional_usd,
        )

        pnl_4h = sim["outcomes"]["4h"]["pnl"] - cost_24h
        pnl_12h = sim["outcomes"]["12h"]["pnl"] - cost_24h
        pnl_24h = sim["outcomes"]["24h"]["pnl"] - cost_24h

        records.append(
            TradeRecord(
                trade_id=str(uuid.uuid4()),
                signal_id=None,
                symbol=symbol,
                signal_ts=signal_ts,
                entry_ts=entry_ts,
                entry_price=float(sim["entry_price"]),
                size_usd=settings.notional_usd,
                units=float(sim["units"]),
                stop_price=float(stop_price),
                take_profit_price=float(take_profit_price),
                slippage_bps=float(slippage_bps),
                funding_cost_usd=float(cost_24h),
                pnl_4h=float(pnl_4h),
                pnl_12h=float(pnl_12h),
                pnl_24h=float(pnl_24h),
                win_4h=bool(pnl_4h > 0),
                win_12h=bool(pnl_12h > 0),
                win_24h=bool(pnl_24h > 0),
                baseline_flag=True,
                btc_regime="unknown",
                funding_env="neutral",
                distribution_flag=False,
                entry_delay_candles=entry_delay_candles,
                corr7d_pre_signal=np.nan,
                corr7d_drop_type="unknown",
            )
        )

    return records


def bootstrap_baseline_pvalues(trades: pd.DataFrame, horizon: str = "24h", n_bootstrap: int = 2000) -> dict:
    pnl_col = f"pnl_{horizon}"
    win_col = f"win_{horizon}"
    signal = trades[~trades["baseline_flag"]]
    baseline = trades[trades["baseline_flag"]]
    if signal.empty or baseline.empty:
        return {
            "horizon": horizon,
            "observed_pnl_diff": np.nan,
            "pnl_p_value": np.nan,
            "observed_win_diff": np.nan,
            "win_p_value": np.nan,
        }

    observed_pnl_diff = signal[pnl_col].mean() - baseline[pnl_col].mean()
    observed_win_diff = signal[win_col].mean() - baseline[win_col].mean()

    rng = np.random.default_rng(123)
    null_pnl_diffs = []
    null_win_diffs = []

    baseline_vals = baseline[pnl_col].to_numpy()
    baseline_win_vals = baseline[win_col].astype(float).to_numpy()
    n = len(signal)

    for _ in range(n_bootstrap):
        sample_a = rng.choice(baseline_vals, size=n, replace=True)
        sample_b = rng.choice(baseline_vals, size=n, replace=True)
        null_pnl_diffs.append(sample_a.mean() - sample_b.mean())

        sample_wa = rng.choice(baseline_win_vals, size=n, replace=True)
        sample_wb = rng.choice(baseline_win_vals, size=n, replace=True)
        null_win_diffs.append(sample_wa.mean() - sample_wb.mean())

    pnl_p_value = float(np.mean(np.abs(null_pnl_diffs) >= abs(observed_pnl_diff)))
    win_p_value = float(np.mean(np.abs(null_win_diffs) >= abs(observed_win_diff)))

    return {
        "horizon": horizon,
        "observed_pnl_diff": float(observed_pnl_diff),
        "pnl_p_value": pnl_p_value,
        "observed_win_diff": float(observed_win_diff),
        "win_p_value": win_p_value,
    }
