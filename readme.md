## BTC-Alt Divergence Detection & Signal Validation System

This project implements an honest divergence-research workflow between `BTCUSDT` perpetual futures and a configurable alt basket (`ETHUSDT`, `SOLUSDT`, `BNBUSDT`, `AVAXUSDT`, `MATICUSDT`, `ARBUSDT`) using Binance Futures public REST endpoints.

### What it includes

- Data ingestion (`1m`, `5m`, `1h` candles; open interest history; funding history)
- Strict DuckDB schema with separate `event_ts` and `ingestion_ts`
- Divergence engine with rolling correlation/beta + swing/OI/funding/regime filters
- Simulation layer with delayed entry, stop/take-profit, slippage, funding costs
- Baseline random-entry comparison + bootstrap p-values
- Regime and funding stratification + failure-mode analysis
- FastAPI backend and Streamlit dashboard
- CSV export of all signals with features + `FINDINGS.md`

### Install

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Run pipeline

```bash
python -m scripts.run_pipeline
python -m scripts.export_signals
python -m scripts.generate_findings
```

### Start backend

```bash
uvicorn app.api:app --reload
```

### Start dashboard

```bash
streamlit run app/dashboard.py
```

### Key files

- `app/ingestion.py` – Binance Futures ingestion and storage
- `app/divergence.py` – divergence event detection
- `app/simulation.py` – simulation + baseline + significance
- `app/analytics.py` – breakdowns and failure-mode analysis
- `app/api.py` – FastAPI endpoints
- `app/dashboard.py` – Streamlit frontend
- `scripts/export_signals.py` – CSV export for independent validation
- `scripts/generate_findings.py` – writes `FINDINGS.md`