from __future__ import annotations

from fastapi import FastAPI

from app.analytics import (
    findings_snapshot,
    live_correlation_heatmap_data,
    load_latest_signals,
    simulation_breakdowns,
    where_this_breaks,
)


app = FastAPI(title="BTC-Alt Divergence Validation API", version="1.0.0")


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.get("/heatmap")
def heatmap() -> list[dict]:
    return live_correlation_heatmap_data().to_dict(orient="records")


@app.get("/signals/active")
def active_signals(limit: int = 50) -> list[dict]:
    df = load_latest_signals().head(limit)
    return df.to_dict(orient="records")


@app.get("/simulation/breakdowns")
def breakdowns() -> dict:
    result = simulation_breakdowns()
    return {
        "by_coin": result["by_coin"].to_dict(orient="records"),
        "by_regime": result["by_regime"].to_dict(orient="records"),
        "by_funding": result["by_funding"].to_dict(orient="records"),
        "baseline": result["baseline"],
    }


@app.get("/analysis/where-breaks")
def where_breaks() -> dict:
    result = where_this_breaks()
    return {
        "worst_signals": result["worst_signals"].to_dict(orient="records"),
        "delay_decay": result["delay_decay"].to_dict(orient="records"),
        "corr_regime": result["corr_regime"].to_dict(orient="records"),
        "distribution_cases": result["distribution_cases"].to_dict(orient="records"),
    }


@app.get("/findings/snapshot")
def findings() -> dict:
    return findings_snapshot()
