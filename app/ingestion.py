from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable
import time

import pandas as pd
import requests
from requests import HTTPError

from app.config import settings
from app.db import get_connection


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


@dataclass
class BinanceFuturesClient:
    base_url: str = settings.base_url
    timeout: int = 30
    pause_sec: float = 0.15

    def _get(self, path: str, params: dict) -> list | dict:
        url = f"{self.base_url}{path}"
        response = requests.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        time.sleep(self.pause_sec)
        return response.json()

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 1500,
    ) -> list[list]:
        all_rows: list[list] = []
        cursor = start_time_ms
        while cursor < end_time_ms:
            rows = self._get(
                "/fapi/v1/klines",
                {
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": end_time_ms,
                    "limit": limit,
                },
            )
            if not rows:
                break
            all_rows.extend(rows)
            last_open_time = rows[-1][0]
            cursor = last_open_time + 1
            if len(rows) < limit:
                break
        return all_rows

    def fetch_open_interest_hist(
        self,
        symbol: str,
        period: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 500,
    ) -> list[dict]:
        all_rows: list[dict] = []
        chunk_ms = 30 * 24 * 60 * 60 * 1000
        chunk_start = start_time_ms

        while chunk_start < end_time_ms:
            chunk_end = min(chunk_start + chunk_ms, end_time_ms)
            cursor = chunk_start

            while cursor < chunk_end:
                try:
                    rows = self._get(
                        "/futures/data/openInterestHist",
                        {
                            "symbol": symbol,
                            "period": period,
                            "startTime": cursor,
                            "endTime": chunk_end,
                            "limit": limit,
                        },
                    )
                except HTTPError:
                    break

                if not rows:
                    break
                all_rows.extend(rows)
                last_ts = int(rows[-1]["timestamp"])
                cursor = last_ts + 1
                if len(rows) < limit:
                    break

            chunk_start = chunk_end + 1

        if all_rows:
            dedup = {int(row["timestamp"]): row for row in all_rows}
            return [dedup[k] for k in sorted(dedup.keys())]
        return all_rows

    def fetch_funding_rates(
        self,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 1000,
    ) -> list[dict]:
        all_rows: list[dict] = []
        cursor = start_time_ms
        while cursor < end_time_ms:
            rows = self._get(
                "/fapi/v1/fundingRate",
                {
                    "symbol": symbol,
                    "startTime": cursor,
                    "endTime": end_time_ms,
                    "limit": limit,
                },
            )
            if not rows:
                break
            all_rows.extend(rows)
            last_ts = int(rows[-1]["fundingTime"])
            cursor = last_ts + 1
            if len(rows) < limit:
                break
        return all_rows


def _candles_to_df(rows: Iterable[list], symbol: str, interval: str) -> pd.DataFrame:
    ingestion_ts = utc_now()
    records = []
    for row in rows:
        records.append(
            {
                "symbol": symbol,
                "interval": interval,
                "event_ts": ms_to_dt(int(row[0])),
                "close_ts": ms_to_dt(int(row[6])),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "quote_volume": float(row[7]),
                "trade_count": int(row[8]),
                "taker_buy_base": float(row[9]),
                "taker_buy_quote": float(row[10]),
                "ingestion_ts": ingestion_ts,
            }
        )
    return pd.DataFrame.from_records(records)


def _open_interest_to_df(rows: Iterable[dict], symbol: str, interval: str) -> pd.DataFrame:
    ingestion_ts = utc_now()
    records = []
    for row in rows:
        records.append(
            {
                "symbol": symbol,
                "interval": interval,
                "event_ts": ms_to_dt(int(row["timestamp"])),
                "open_interest": float(row["sumOpenInterest"]),
                "open_interest_value": float(row["sumOpenInterestValue"]),
                "ingestion_ts": ingestion_ts,
            }
        )
    return pd.DataFrame.from_records(records)


def _funding_to_df(rows: Iterable[dict], symbol: str) -> pd.DataFrame:
    ingestion_ts = utc_now()
    records = []
    for row in rows:
        records.append(
            {
                "symbol": symbol,
                "event_ts": ms_to_dt(int(row["fundingTime"])),
                "funding_rate": float(row["fundingRate"]),
                "mark_price": float(row.get("markPrice", 0.0)) if row.get("markPrice") else None,
                "ingestion_ts": ingestion_ts,
            }
        )
    return pd.DataFrame.from_records(records)


def _upsert_df(conn, df: pd.DataFrame, table: str) -> None:
    if df.empty:
        return
    conn.register("tmp_df", df)
    conn.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM tmp_df")
    conn.unregister("tmp_df")


def backfill_history(lookback_days: int = settings.lookback_days) -> None:
    client = BinanceFuturesClient()
    conn = get_connection(settings.db_path)

    end = utc_now()
    start = end - timedelta(days=lookback_days)
    start_ms = dt_to_ms(start)
    end_ms = dt_to_ms(end)

    for symbol in settings.symbols:
        for interval in settings.candle_intervals:
            rows = client.fetch_klines(symbol, interval, start_ms, end_ms)
            _upsert_df(conn, _candles_to_df(rows, symbol, interval), "candles")

        oi_rows = client.fetch_open_interest_hist(symbol, "5m", start_ms, end_ms)
        _upsert_df(conn, _open_interest_to_df(oi_rows, symbol, "5m"), "open_interest")

        funding_rows = client.fetch_funding_rates(symbol, start_ms, end_ms)
        _upsert_df(conn, _funding_to_df(funding_rows, symbol), "funding_rates")

    conn.close()
