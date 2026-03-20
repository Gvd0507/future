from __future__ import annotations

from pathlib import Path
import pandas as pd

from app.config import settings
from app.db import get_connection


def main() -> None:
    conn = get_connection(settings.db_path)
    df = conn.execute(
        """
        SELECT s.*, t.trade_id, t.entry_ts, t.entry_price, t.stop_price, t.take_profit_price,
               t.slippage_bps, t.funding_cost_usd, t.pnl_4h, t.pnl_12h, t.pnl_24h,
               t.win_4h, t.win_12h, t.win_24h, t.btc_regime as trade_btc_regime,
               t.funding_env, t.distribution_flag, t.entry_delay_candles,
               t.corr7d_pre_signal, t.corr7d_drop_type
        FROM divergence_signals s
        LEFT JOIN simulated_trades t
          ON s.signal_id = t.signal_id
        WHERE t.baseline_flag = FALSE
        ORDER BY s.signal_ts
        """
    ).df()
    conn.close()

    out_dir = Path(settings.raw_export_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "signals_with_features.csv"
    df.to_csv(out_path, index=False)
    print(f"Exported {len(df)} rows to {out_path}")


if __name__ == "__main__":
    main()
