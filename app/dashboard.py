from __future__ import annotations

import os
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

from app.db import get_connection
from app.config import settings


st.set_page_config(layout="wide", page_title="BTC-Alt Divergence Validation")
st.title("BTC-Alt Divergence Detection & Honest Validation")

API_URL = os.getenv("API_URL", "http://127.0.0.1:8000")


def fetch(endpoint: str):
    try:
        r = requests.get(f"{API_URL}{endpoint}", timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


st.subheader("Live Rolling Correlation Heatmap")
heatmap_data = fetch("/heatmap") or []
heatmap_df = pd.DataFrame(heatmap_data)

if heatmap_df.empty:
    st.info("No heatmap data yet. Run ingestion + signal engine first.")
else:
    tabs = st.tabs(["4h", "24h", "7d"])
    for tab, w in zip(tabs, ["4h", "24h", "7d"]):
        with tab:
            t = heatmap_df[heatmap_df["window"] == w]
            pivot = t.pivot_table(index="symbol", values="correlation")
            st.dataframe(pivot, use_container_width=True)


st.subheader("Active Divergence Signals")
signals = pd.DataFrame(fetch("/signals/active") or [])
if signals.empty:
    st.info("No active divergence signals stored.")
else:
    cols = [
        "signal_ts",
        "symbol",
        "divergence_type",
        "signal_score",
        "beta_4h",
        "oi_delta_zscore",
        "funding_rate",
        "btc_regime",
    ]
    st.dataframe(signals[cols], use_container_width=True)


st.subheader("Simulation Results")
breakdowns = fetch("/simulation/breakdowns") or {}
by_coin = pd.DataFrame(breakdowns.get("by_coin", []))
by_regime = pd.DataFrame(breakdowns.get("by_regime", []))
by_funding = pd.DataFrame(breakdowns.get("by_funding", []))
baseline = pd.DataFrame(breakdowns.get("baseline", []))

c1, c2 = st.columns(2)
with c1:
    st.markdown("**By Coin**")
    st.dataframe(by_coin, use_container_width=True)
    st.markdown("**By BTC Regime**")
    st.dataframe(by_regime, use_container_width=True)
with c2:
    st.markdown("**By Funding Environment**")
    st.dataframe(by_funding, use_container_width=True)
    st.markdown("**Baseline Comparison (Bootstrap p-values)**")
    st.dataframe(baseline, use_container_width=True)
    if not baseline.empty:
        fail = ((baseline["pnl_p_value"] >= 0.05) | (baseline["win_p_value"] >= 0.05)).any()
        if fail:
            st.error("Divergence signal does NOT beat baseline at p < 0.05 on at least one key metric.")
        else:
            st.success("Divergence signal beats baseline at p < 0.05 on both key metrics.")


st.subheader("Candlestick + Signal Markers (Winners and Losers)")
conn = get_connection(settings.db_path)
candles = conn.execute(
    """
    SELECT event_ts, open, high, low, close
    FROM candles
    WHERE symbol='ETHUSDT' AND interval='5m'
    ORDER BY event_ts DESC
    LIMIT 500
    """
).df().sort_values("event_ts")
trades = conn.execute(
    """
    SELECT symbol, entry_ts, entry_price, win_24h
    FROM simulated_trades
    WHERE baseline_flag = FALSE AND symbol='ETHUSDT'
    ORDER BY entry_ts DESC
    LIMIT 200
    """
).df()
conn.close()

if candles.empty:
    st.info("No candles to draw chart yet.")
else:
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=candles["event_ts"],
                open=candles["open"],
                high=candles["high"],
                low=candles["low"],
                close=candles["close"],
                name="ETHUSDT",
            )
        ]
    )
    if not trades.empty:
        wins = trades[trades["win_24h"] == True]
        losses = trades[trades["win_24h"] == False]
        fig.add_trace(
            go.Scatter(
                x=wins["entry_ts"],
                y=wins["entry_price"],
                mode="markers",
                marker=dict(size=8, symbol="triangle-up"),
                name="Winning Signals",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=losses["entry_ts"],
                y=losses["entry_price"],
                mode="markers",
                marker=dict(size=8, symbol="triangle-down"),
                name="Losing Signals",
            )
        )
    fig.update_layout(height=500, xaxis_rangeslider_visible=False)
    st.plotly_chart(fig, use_container_width=True)


st.subheader("Where This Breaks")
breaks = fetch("/analysis/where-breaks") or {}
worst = pd.DataFrame(breaks.get("worst_signals", []))
delay = pd.DataFrame(breaks.get("delay_decay", []))
corr = pd.DataFrame(breaks.get("corr_regime", []))
dist = pd.DataFrame(breaks.get("distribution_cases", []))

st.markdown("**5 worst-performing signals**")
st.dataframe(worst, use_container_width=True)
st.markdown("**Time-decay of quality (entry delay candles)**")
st.dataframe(delay, use_container_width=True)
st.markdown("**Correlation regime breakdown (already low vs sudden drop)**")
st.dataframe(corr, use_container_width=True)
st.markdown("**Potential distribution signals (OI up, price against long within 30m)**")
st.dataframe(dist, use_container_width=True)
