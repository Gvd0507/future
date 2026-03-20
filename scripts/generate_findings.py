from __future__ import annotations

from pathlib import Path
import pandas as pd

from app.analytics import findings_snapshot, load_latest_trades


def _fmt(df: pd.DataFrame) -> str:
    if df.empty:
        return "No data available."
    return df.to_markdown(index=False)


def main() -> None:
    snapshot = findings_snapshot()
    trades = load_latest_trades()
    signal_trades = trades[~trades["baseline_flag"]] if not trades.empty else pd.DataFrame()

    baseline = pd.DataFrame(snapshot.get("baseline", []))
    regime = pd.DataFrame(snapshot.get("regime", []))
    funding = pd.DataFrame(snapshot.get("funding", []))
    worst = pd.DataFrame(snapshot.get("worst_signals", []))

    conclusion = "INCONCLUSIVE"
    if not baseline.empty:
        if ((baseline["pnl_p_value"] < 0.05) & (baseline["win_p_value"] < 0.05)).all():
            conclusion = "POTENTIAL EDGE"
        else:
            conclusion = "LIKELY BACKTEST ARTIFACT"

    content = f"""# FINDINGS

## Honest Summary

- Total divergence trades evaluated: {len(signal_trades)}
- Total baseline trades evaluated: {len(trades[trades['baseline_flag']]) if not trades.empty else 0}
- Current assessment: **{conclusion}**

If divergence does not beat baseline at p < 0.05, treat any apparent outperformance as noise.

## Baseline Comparison (Bootstrap p-values)

{_fmt(baseline)}

## Regime Dependency (Trending vs Ranging)

{_fmt(regime)}

## Funding Environment Split (Positive / Neutral / Negative)

{_fmt(funding)}

## Where This Breaks

### Worst Signals

{_fmt(worst)}

### Distribution-Signal Risk

- Distribution-case ratio: {snapshot.get('distribution_case_ratio', float('nan')):.4f}
- Interpretation: higher values indicate more frequent wrong-way OI expansion (possible informed distribution into strength).

## What Works vs What Doesn't

- Works only if evidence says so in regime/funding tables above.
- Fails where p-values are not significant or where adverse regime concentration is present.
- Treat any strategy that is regime-fragile as conditional, not universal.

## Methodological Caveats

- Look-ahead control is enforced via confirmed swing construction and delayed entry.
- Slippage is estimated empirically from candle-range microstructure proxy, because historical full-depth bid/ask snapshots are not directly available from the used public endpoints.
- Findings should be re-generated whenever new history is ingested.
"""

    Path("FINDINGS.md").write_text(content, encoding="utf-8")
    print("Wrote FINDINGS.md")


if __name__ == "__main__":
    main()
