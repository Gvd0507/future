# FINDINGS

## Honest Summary

- Total divergence trades evaluated: 0
- Total baseline trades evaluated: 0
- Current assessment: **INCONCLUSIVE**

If divergence does not beat baseline at p < 0.05, treat any apparent outperformance as noise.

## Baseline Comparison (Bootstrap p-values)

No data available.

## Regime Dependency (Trending vs Ranging)

No data available.

## Funding Environment Split (Positive / Neutral / Negative)

No data available.

## Where This Breaks

### Worst Signals

No data available.

### Distribution-Signal Risk

- Distribution-case ratio: 0.0000
- Interpretation: higher values indicate more frequent wrong-way OI expansion (possible informed distribution into strength).

## What Works vs What Doesn't

- Works only if evidence says so in regime/funding tables above.
- Fails where p-values are not significant or where adverse regime concentration is present.
- Treat any strategy that is regime-fragile as conditional, not universal.

## Methodological Caveats

- Look-ahead control is enforced via confirmed swing construction and delayed entry.
- Slippage is estimated empirically from candle-range microstructure proxy, because historical full-depth bid/ask snapshots are not directly available from the used public endpoints.
- Findings should be re-generated whenever new history is ingested.
