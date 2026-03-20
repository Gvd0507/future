from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    base_url: str = "https://fapi.binance.com"
    db_path: str = str(Path("data") / "divergence.duckdb")
    raw_export_path: str = str(Path("data") / "exports")
    symbols: tuple[str, ...] = (
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "BNBUSDT",
        "AVAXUSDT",
        "MATICUSDT",
        "ARBUSDT",
    )
    btc_symbol: str = "BTCUSDT"
    alt_symbols: tuple[str, ...] = (
        "ETHUSDT",
        "SOLUSDT",
        "BNBUSDT",
        "AVAXUSDT",
        "MATICUSDT",
        "ARBUSDT",
    )
    candle_intervals: tuple[str, ...] = ("1m", "5m", "1h")
    lookback_days: int = 180
    correlation_windows_bars_5m: dict[str, int] = None
    adx_period: int = 14
    adx_trending_threshold: float = 25.0
    swing_confirmation_candles: int = 3
    notional_usd: float = 10_000.0
    stop_buffer_pct: float = 0.005
    tp_btc_move_mult: float = 1.5

    def __post_init__(self):
        if self.correlation_windows_bars_5m is None:
            object.__setattr__(
                self,
                "correlation_windows_bars_5m",
                {
                    "4h": 48,
                    "24h": 288,
                    "7d": 2016,
                },
            )


settings = Settings()
