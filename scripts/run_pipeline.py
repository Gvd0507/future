from __future__ import annotations

from app.divergence import detect_divergence_signals
from app.ingestion import backfill_history
from app.simulation import run_simulation


def main() -> None:
    backfill_history()
    signals = detect_divergence_signals()
    print(f"Detected signals: {len(signals)}")
    trades = run_simulation(entry_delay_candles=2)
    print(f"Simulated trades (signal + baseline): {len(trades)}")


if __name__ == "__main__":
    main()
