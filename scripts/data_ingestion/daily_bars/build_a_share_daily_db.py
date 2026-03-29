#!/usr/bin/env python
"""
Build/update a local SQLite database for daily A-share trend-following research.

Usage examples:
  python scripts/build_a_share_daily_db.py
  python scripts/build_a_share_daily_db.py --init
  python scripts/build_a_share_daily_db.py --update
  python scripts/build_a_share_daily_db.py --update --lookback-days 15

Auth:
  - Prefer env var: TUSHARE_TOKEN
  - Or pass --token directly
  - Or use local secret file (default: secrets/tushare.env)
  - Optional mirror URL: --http-url or env TUSHARE_HTTP_URL
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

import pandas as pd
import tushare as ts


LOGGER = logging.getLogger("a_share_daily_db")


# Edit once, then run with short commands.
USER_DEFAULTS = {
    "mode": "init",  # Run mode: "init" for full load, "update" for incremental
    "secret_file": "secrets/tushare.env",  # Local secret file with TUSHARE_TOKEN/TUSHARE_HTTP_URL
    "db_path": "data/daily_bars/a_share_daily.db",  # SQLite database path
    "log_file": "logs/build_a_share_daily_db.log",  # Log file path (warnings and info)
    "start_date": "20100101",  # Full-load start date (YYYYMMDD)
    "end_date": "AUTO_TODAY",  # End date: AUTO_TODAY or explicit YYYYMMDD
    "lookback_days": 10,  # Incremental backfill days
    "min_listed_days": 120,  # Universe filter: minimum listed days
    "min_amount": 20_000_000.0,  # Universe filter: minimum daily amount
    "min_price": 3.0,  # Universe filter: minimum close price
    "retries": 5,  # API retry count
    "sleep_sec": 0.4,  # Sleep seconds between API calls
    "timeout_sec": 30,  # HTTP request timeout seconds
    "namechange_chunk_days": 30,  # Chunk size (days) for namechange endpoint
    "skip_namechange": True,  # Skip namechange refresh for faster runs
    "workers": 20,  # Parallel worker threads for daily pulling
    "max_inflight": 30,  # Max in-flight trade dates (should be >= workers)
    "verbose": False,  # True=DEBUG logs, False=INFO logs
    "per_date_retries": 5,  # Auto re-pull retries when one day is incomplete
    "precheck_existing": True,  # Precheck existing data and skip complete dates
    "repair_only_missing": False,  # False: also backfill all-empty dates; True: only repair partially-missing dates
    "unresolved_rounds": 2,  # Extra post-pass repair rounds for dates still missing after per-date retries
    "unresolved_cooldown_sec": 1.0,  # Sleep seconds between unresolved-date repairs (reduce API burst)
    "adaptive_concurrency": False,  # Auto-tune in-flight concurrency by observed API error/timeout rates
    "adaptive_window": 40,  # Evaluate adaptive concurrency every N written trade dates
    "adaptive_min_inflight": 8,  # Minimum in-flight cap during auto downshift
    "adaptive_reduce_factor": 0.75,  # Multiplicative downshift factor when API quality degrades
    "adaptive_increase_step": 2,  # Step up in-flight cap when API quality recovers
    "adaptive_high_error_rate": 0.35,  # Downshift if error attempts / calls >= this threshold
    "adaptive_low_error_rate": 0.08,  # Upshift if error attempts / calls <= this threshold
    "adaptive_high_timeout_ratio": 0.60,  # Downshift if timeout errors / all errors >= this threshold
}


def setup_logging(verbose: bool, log_file: str) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    log_path = Path(log_file).resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(file_handler)


class ConsoleProgress:
    def __init__(self, total: int, label: str) -> None:
        self.total = max(1, int(total))
        self.label = label
        self.current = 0
        self.last_len = 0
        self.last_render_ts = 0.0

    def update(self, current: int, extra: str = "", force: bool = False) -> None:
        self.current = max(0, min(int(current), self.total))
        now = time.monotonic()
        # Throttle console updates to avoid excessive line rendering in some terminals.
        if not force and self.current < self.total and (now - self.last_render_ts) < 0.5:
            return

        pct = 100.0 * self.current / self.total
        bar_len = 30
        filled = int(bar_len * self.current / self.total)
        bar = "#" * filled + "-" * (bar_len - filled)
        msg = f"{self.label} [{bar}] {self.current}/{self.total} ({pct:5.1f}%)"
        if extra:
            msg += f" | {extra}"
        pad = max(0, self.last_len - len(msg))
        print("\r" + msg + " " * pad, end="", flush=True)
        self.last_len = len(msg)
        self.last_render_ts = now

    def close(self, extra: str = "") -> None:
        self.update(self.current, extra, force=True)
        print(file=sys.stdout, flush=True)

@dataclass
class Config:
    token: str
    http_url: Optional[str]
    db_path: Path
    log_file: str
    start_date: str
    end_date: str
    min_listed_days: int
    min_amount: float
    min_price: float
    retries: int
    sleep_sec: float
    workers: int
    max_inflight: int
    timeout_sec: int
    namechange_chunk_days: int
    skip_namechange: bool
    per_date_retries: int
    precheck_existing: bool
    repair_only_missing: bool
    unresolved_rounds: int
    unresolved_cooldown_sec: float
    adaptive_concurrency: bool
    adaptive_window: int
    adaptive_min_inflight: int
    adaptive_reduce_factor: float
    adaptive_increase_step: int
    adaptive_high_error_rate: float
    adaptive_low_error_rate: float
    adaptive_high_timeout_ratio: float

@dataclass
class DailyFetchBundle:
    trade_date: str
    daily_df: pd.DataFrame
    adj_df: pd.DataFrame
    basic_df: pd.DataFrame
    limit_df: pd.DataFrame
    suspend_df: pd.DataFrame


_THREAD_LOCAL = threading.local()
_API_STATS_LOCK = threading.Lock()
_API_STATS = {"calls": 0, "errors": 0, "timeouts": 0}


def reset_api_stats() -> None:
    with _API_STATS_LOCK:
        _API_STATS["calls"] = 0
        _API_STATS["errors"] = 0
        _API_STATS["timeouts"] = 0


def snapshot_api_stats() -> tuple[int, int, int]:
    with _API_STATS_LOCK:
        return (
            int(_API_STATS["calls"]),
            int(_API_STATS["errors"]),
            int(_API_STATS["timeouts"]),
        )


def is_timeout_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "timeout" in text
        or "timed out" in text
        or "connecttimeout" in text
        or "read timeout" in text
    )



def today_yyyymmdd() -> str:
    return dt.datetime.now().strftime("%Y%m%d")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)



def load_simple_env(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        txt = line.strip()
        if not txt or txt.startswith("#") or "=" not in txt:
            continue
        k, v = txt.split("=", 1)
        key = k.strip().lstrip("\ufeff")
        val = v.strip().strip('\"').strip("'")
        if key:
            out[key] = val
    return out


def create_pro_client(token: str, http_url: Optional[str], timeout_sec: Optional[int] = None) -> ts.pro_api:
    pro = ts.pro_api(token)
    if http_url:
        # Private mirror / proxy endpoint if needed.
        pro._DataApi__http_url = http_url  # pylint: disable=protected-access
    if timeout_sec and timeout_sec > 0:
        # Increase request timeout for slow endpoints (e.g., namechange).
        pro._DataApi__timeout = int(timeout_sec)  # pylint: disable=protected-access
    return pro


def get_thread_pro_client(cfg: Config):
    pro = getattr(_THREAD_LOCAL, "pro", None)
    if pro is None:
        pro = create_pro_client(cfg.token, cfg.http_url, cfg.timeout_sec)
        _THREAD_LOCAL.pro = pro
    return pro


def create_schema(conn: sqlite3.Connection) -> None:
    ddl = """
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS dim_security (
        ts_code TEXT PRIMARY KEY,
        symbol TEXT,
        name TEXT,
        area TEXT,
        industry TEXT,
        market TEXT,
        list_status TEXT,
        list_date TEXT,
        delist_date TEXT
    );

    CREATE TABLE IF NOT EXISTS fact_trade_cal (
        exchange TEXT,
        cal_date TEXT,
        is_open INTEGER,
        pretrade_date TEXT,
        PRIMARY KEY (exchange, cal_date)
    );

    CREATE TABLE IF NOT EXISTS fact_daily_bar (
        ts_code TEXT,
        trade_date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        pre_close REAL,
        change REAL,
        pct_chg REAL,
        vol REAL,
        amount REAL,
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS fact_adj_factor (
        ts_code TEXT,
        trade_date TEXT,
        adj_factor REAL,
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS fact_daily_basic (
        ts_code TEXT,
        trade_date TEXT,
        turnover_rate REAL,
        turnover_rate_f REAL,
        volume_ratio REAL,
        pe REAL,
        pe_ttm REAL,
        pb REAL,
        ps REAL,
        ps_ttm REAL,
        dv_ratio REAL,
        dv_ttm REAL,
        total_share REAL,
        float_share REAL,
        free_share REAL,
        total_mv REAL,
        circ_mv REAL,
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS fact_stk_limit (
        ts_code TEXT,
        trade_date TEXT,
        pre_close REAL,
        up_limit REAL,
        down_limit REAL,
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS fact_suspend_d (
        ts_code TEXT,
        trade_date TEXT,
        suspend_timing TEXT,
        suspend_type TEXT,
        PRIMARY KEY (ts_code, trade_date, suspend_timing)
    );

    CREATE TABLE IF NOT EXISTS fact_namechange (
        ts_code TEXT,
        name TEXT,
        start_date TEXT,
        end_date TEXT,
        ann_date TEXT,
        change_reason TEXT,
        PRIMARY KEY (ts_code, start_date, name)
    );

    CREATE TABLE IF NOT EXISTS fact_universe_pti (
        ts_code TEXT,
        trade_date TEXT,
        is_listed_enough INTEGER,
        is_liquid_enough INTEGER,
        is_price_ok INTEGER,
        is_suspended INTEGER,
        is_st INTEGER,
        in_universe INTEGER,
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE INDEX IF NOT EXISTS idx_daily_trade_date ON fact_daily_bar(trade_date);
    CREATE INDEX IF NOT EXISTS idx_adj_trade_date ON fact_adj_factor(trade_date);
    CREATE INDEX IF NOT EXISTS idx_daily_basic_trade_date ON fact_daily_basic(trade_date);
    CREATE INDEX IF NOT EXISTS idx_stk_limit_trade_date ON fact_stk_limit(trade_date);
    CREATE INDEX IF NOT EXISTS idx_suspend_trade_date ON fact_suspend_d(trade_date);
    CREATE INDEX IF NOT EXISTS idx_universe_trade_date ON fact_universe_pti(trade_date);
    """
    conn.executescript(ddl)
    conn.commit()


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def upsert_df(
    conn: sqlite3.Connection, table: str, df: pd.DataFrame, keep_cols: Optional[Sequence[str]] = None
) -> int:
    if df is None or df.empty:
        return 0
    if keep_cols is not None:
        cols = [c for c in keep_cols if c in df.columns]
        if not cols:
            return 0
        df = df.loc[:, cols].copy()
    tmp = f"_tmp_{table}_{int(time.time() * 1000)}"
    df.to_sql(tmp, conn, if_exists="replace", index=False)
    cols = list(df.columns)
    col_csv = ", ".join(quote_ident(c) for c in cols)
    conn.execute(
        f"INSERT OR REPLACE INTO {quote_ident(table)} ({col_csv}) "
        f"SELECT {col_csv} FROM {quote_ident(tmp)}"
    )
    conn.execute(f"DROP TABLE {quote_ident(tmp)}")
    conn.commit()
    return len(df)


def safe_call(func, retries: int, sleep_sec: float, **kwargs) -> pd.DataFrame:
    last_err = None
    with _API_STATS_LOCK:
        _API_STATS["calls"] += 1
    for i in range(retries):
        try:
            df = func(**kwargs)
            return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
        except Exception as exc:  # pylint: disable=broad-except
            last_err = exc
            with _API_STATS_LOCK:
                _API_STATS["errors"] += 1
                if is_timeout_error(exc):
                    _API_STATS["timeouts"] += 1
            wait = sleep_sec * (i + 1)
            LOGGER.warning("API call failed (%s), retrying in %.1fs ...", exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"API call failed after {retries} retries: {last_err}") from last_err


def fetch_stock_basic(pro, cfg: Config) -> pd.DataFrame:
    frames = []
    for status in ("L", "D", "P"):
        df = safe_call(
            pro.stock_basic,
            retries=cfg.retries,
            sleep_sec=cfg.sleep_sec,
            exchange="",
            list_status=status,
            fields="ts_code,symbol,name,area,industry,market,list_status,list_date,delist_date",
        )
        if not df.empty:
            frames.append(df)
        time.sleep(cfg.sleep_sec)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts_code"], keep="last")
    return out


def fetch_trade_cal(pro, cfg: Config) -> pd.DataFrame:
    return safe_call(
        pro.trade_cal,
        retries=cfg.retries,
        sleep_sec=cfg.sleep_sec,
        exchange="SSE",
        start_date=cfg.start_date,
        end_date=cfg.end_date,
    )


def iter_date_chunks(start_date: str, end_date: str, chunk_days: int) -> list[tuple[str, str]]:
    s = dt.datetime.strptime(start_date, "%Y%m%d")
    e = dt.datetime.strptime(end_date, "%Y%m%d")
    out: list[tuple[str, str]] = []
    step = max(1, int(chunk_days))
    while s <= e:
        ce = min(s + dt.timedelta(days=step - 1), e)
        out.append((s.strftime("%Y%m%d"), ce.strftime("%Y%m%d")))
        s = ce + dt.timedelta(days=1)
    return out


def fetch_namechange(pro, cfg: Config) -> pd.DataFrame:
    chunks = iter_date_chunks(cfg.start_date, cfg.end_date, cfg.namechange_chunk_days)
    frames: list[pd.DataFrame] = []
    failed_chunks = 0
    progress = ConsoleProgress(len(chunks), "namechange")
    progress.update(0, "starting", force=True)
    progress_step = max(1, len(chunks) // 50)

    for idx, (s, e) in enumerate(chunks, start=1):
        try:
            df = safe_call(
                pro.namechange,
                retries=cfg.retries,
                sleep_sec=cfg.sleep_sec,
                start_date=s,
                end_date=e,
                fields="ts_code,name,start_date,end_date,ann_date,change_reason",
            )
        except Exception as exc:  # pylint: disable=broad-except
            failed_chunks += 1
            LOGGER.warning("namechange chunk failed (%s -> %s): %s", s, e, exc)
            if idx % progress_step == 0 or idx == len(chunks):
                progress.update(idx, f"failed={failed_chunks}")
            continue

        if not df.empty:
            frames.append(df)
        if idx % progress_step == 0 or idx == len(chunks):
            progress.update(idx, f"failed={failed_chunks}")
        time.sleep(cfg.sleep_sec)

    progress.close(f"failed={failed_chunks}")

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["ts_code", "start_date", "name"], keep="last")
    return out


def get_open_dates(conn: sqlite3.Connection, start_date: str, end_date: str) -> list[str]:
    sql = """
    SELECT cal_date
    FROM fact_trade_cal
    WHERE exchange='SSE' AND is_open=1 AND cal_date BETWEEN ? AND ?
    ORDER BY cal_date
    """
    return [r[0] for r in conn.execute(sql, (start_date, end_date)).fetchall()]


def get_st_codes_on_date(conn: sqlite3.Connection, trade_date: str) -> set[str]:
    sql = """
    SELECT DISTINCT ts_code
    FROM fact_namechange
    WHERE start_date <= ?
      AND (end_date IS NULL OR end_date = '' OR end_date >= ?)
      AND UPPER(name) LIKE '%ST%'
    """
    return {r[0] for r in conn.execute(sql, (trade_date, trade_date)).fetchall()}


def build_universe_for_date(
    trade_date: str,
    daily_df: pd.DataFrame,
    suspend_df: pd.DataFrame,
    stock_basic_df: pd.DataFrame,
    st_codes: set[str],
    cfg: Config,
) -> pd.DataFrame:
    if daily_df.empty:
        return pd.DataFrame()

    sec = stock_basic_df[["ts_code", "list_date", "delist_date"]].copy()
    sec["list_date"] = pd.to_numeric(sec["list_date"], errors="coerce")
    sec["delist_date"] = pd.to_numeric(sec["delist_date"], errors="coerce")

    out = daily_df[["ts_code", "trade_date", "close", "amount"]].copy()
    out = out.merge(sec, on="ts_code", how="left")

    date_num = int(trade_date)
    listed_days = date_num - out["list_date"].fillna(99999999)
    out["is_listed_enough"] = (listed_days >= cfg.min_listed_days).astype(int)

    not_delisted = out["delist_date"].isna() | (out["delist_date"] > date_num)
    out["is_listed_enough"] = (out["is_listed_enough"].eq(1) & not_delisted).astype(int)

    out["is_liquid_enough"] = (pd.to_numeric(out["amount"], errors="coerce").fillna(0.0) >= cfg.min_amount).astype(int)
    out["is_price_ok"] = (pd.to_numeric(out["close"], errors="coerce").fillna(0.0) >= cfg.min_price).astype(int)

    suspend_codes = set(suspend_df["ts_code"].tolist()) if not suspend_df.empty and "ts_code" in suspend_df else set()
    out["is_suspended"] = out["ts_code"].isin(suspend_codes).astype(int)
    out["is_st"] = out["ts_code"].isin(st_codes).astype(int)

    out["in_universe"] = (
        (out["is_listed_enough"] == 1)
        & (out["is_liquid_enough"] == 1)
        & (out["is_price_ok"] == 1)
        & (out["is_suspended"] == 0)
        & (out["is_st"] == 0)
    ).astype(int)

    return out[
        [
            "ts_code",
            "trade_date",
            "is_listed_enough",
            "is_liquid_enough",
            "is_price_ok",
            "is_suspended",
            "is_st",
            "in_universe",
        ]
    ]


def max_trade_date(conn: sqlite3.Connection, table: str) -> Optional[str]:
    row = conn.execute(f"SELECT MAX(trade_date) FROM {quote_ident(table)}").fetchone()
    if not row:
        return None
    return row[0]


def update_static_tables(conn: sqlite3.Connection, pro, cfg: Config) -> pd.DataFrame:
    LOGGER.info("Refreshing dim_security ...")
    security_df = fetch_stock_basic(pro, cfg)
    n = upsert_df(
        conn,
        "dim_security",
        security_df,
        keep_cols=["ts_code", "symbol", "name", "area", "industry", "market", "list_status", "list_date", "delist_date"],
    )
    LOGGER.info("dim_security upserted rows: %s", n)

    LOGGER.info("Refreshing fact_trade_cal ...")
    cal_df = fetch_trade_cal(pro, cfg)
    n = upsert_df(conn, "fact_trade_cal", cal_df, keep_cols=["exchange", "cal_date", "is_open", "pretrade_date"])
    LOGGER.info("fact_trade_cal upserted rows: %s", n)

    if cfg.skip_namechange:
        LOGGER.info("Skipping fact_namechange refresh by config (skip_namechange=True)")
        return security_df

    LOGGER.info("Refreshing fact_namechange ...")
    name_df = fetch_namechange(pro, cfg)
    n = upsert_df(
        conn,
        "fact_namechange",
        name_df,
        keep_cols=["ts_code", "name", "start_date", "end_date", "ann_date", "change_reason"],
    )
    LOGGER.info("fact_namechange upserted rows: %s", n)
    return security_df


def fetch_daily_bundle(trade_date: str, cfg: Config) -> DailyFetchBundle:
    pro = get_thread_pro_client(cfg)

    daily_df = safe_call(
        pro.daily,
        retries=cfg.retries,
        sleep_sec=cfg.sleep_sec,
        trade_date=trade_date,
    )
    adj_df = safe_call(
        pro.adj_factor,
        retries=cfg.retries,
        sleep_sec=cfg.sleep_sec,
        trade_date=trade_date,
    )
    basic_df = safe_call(
        pro.daily_basic,
        retries=cfg.retries,
        sleep_sec=cfg.sleep_sec,
        trade_date=trade_date,
    )
    limit_df = safe_call(
        pro.stk_limit,
        retries=cfg.retries,
        sleep_sec=cfg.sleep_sec,
        trade_date=trade_date,
    )
    suspend_df = safe_call(
        pro.suspend_d,
        retries=cfg.retries,
        sleep_sec=cfg.sleep_sec,
        trade_date=trade_date,
    )

    return DailyFetchBundle(
        trade_date=trade_date,
        daily_df=daily_df,
        adj_df=adj_df,
        basic_df=basic_df,
        limit_df=limit_df,
        suspend_df=suspend_df,
    )


def write_daily_bundle(conn: sqlite3.Connection, cfg: Config, bundle: DailyFetchBundle, stock_basic_df: pd.DataFrame) -> None:
    d = bundle.trade_date

    upsert_df(
        conn,
        "fact_daily_bar",
        bundle.daily_df,
        keep_cols=["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"],
    )

    upsert_df(conn, "fact_adj_factor", bundle.adj_df, keep_cols=["ts_code", "trade_date", "adj_factor"])

    upsert_df(
        conn,
        "fact_daily_basic",
        bundle.basic_df,
        keep_cols=[
            "ts_code",
            "trade_date",
            "turnover_rate",
            "turnover_rate_f",
            "volume_ratio",
            "pe",
            "pe_ttm",
            "pb",
            "ps",
            "ps_ttm",
            "dv_ratio",
            "dv_ttm",
            "total_share",
            "float_share",
            "free_share",
            "total_mv",
            "circ_mv",
        ],
    )

    upsert_df(
        conn,
        "fact_stk_limit",
        bundle.limit_df,
        keep_cols=["ts_code", "trade_date", "pre_close", "up_limit", "down_limit"],
    )

    upsert_df(
        conn,
        "fact_suspend_d",
        bundle.suspend_df,
        keep_cols=["ts_code", "trade_date", "suspend_timing", "suspend_type"],
    )

    st_codes = get_st_codes_on_date(conn, d)
    uni_df = build_universe_for_date(d, bundle.daily_df, bundle.suspend_df, stock_basic_df, st_codes, cfg)
    upsert_df(
        conn,
        "fact_universe_pti",
        uni_df,
        keep_cols=[
            "ts_code",
            "trade_date",
            "is_listed_enough",
            "is_liquid_enough",
            "is_price_ok",
            "is_suspended",
            "is_st",
            "in_universe",
        ],
    )


REQUIRED_DAILY_TABLES = (
    "fact_daily_bar",
    "fact_adj_factor",
    "fact_daily_basic",
    "fact_stk_limit",
)


def get_trade_date_counts(conn: sqlite3.Connection, trade_date: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for t in REQUIRED_DAILY_TABLES:
        row = conn.execute(f"SELECT COUNT(*) FROM {quote_ident(t)} WHERE trade_date=?", (trade_date,)).fetchone()
        out[t] = int(row[0] if row else 0)
    return out


def get_table_count(conn: sqlite3.Connection, table: str, trade_date: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {quote_ident(table)} WHERE trade_date=?", (trade_date,)).fetchone()
    return int(row[0] if row else 0)



def missing_tables_from_counts(counts: dict[str, int]) -> list[str]:
    return [t for t, v in counts.items() if int(v) <= 0]

def build_missing_map_for_dates(
    conn: sqlite3.Connection,
    dates: list[str],
    repair_only_missing: bool = False,
) -> tuple[dict[str, list[str]], int, int]:
    if not dates:
        return {}, 0, 0

    min_d = min(dates)
    max_d = max(dates)
    by_table: dict[str, dict[str, int]] = {}

    for t in REQUIRED_DAILY_TABLES:
        rows = conn.execute(
            f"SELECT trade_date, COUNT(*) FROM {quote_ident(t)} WHERE trade_date BETWEEN ? AND ? GROUP BY trade_date",
            (min_d, max_d),
        ).fetchall()
        by_table[t] = {r[0]: int(r[1]) for r in rows}

    missing_map: dict[str, list[str]] = {}
    complete_count = 0
    all_empty_count = 0

    for d in dates:
        missing = [t for t in REQUIRED_DAILY_TABLES if by_table[t].get(d, 0) <= 0]
        if not missing:
            complete_count += 1
            continue

        if repair_only_missing and len(missing) == len(REQUIRED_DAILY_TABLES):
            all_empty_count += 1
            continue

        missing_map[d] = missing

    return missing_map, complete_count, all_empty_count


def fetch_missing_components(
    trade_date: str,
    cfg: Config,
    missing_tables: list[str],
    current: DailyFetchBundle,
) -> DailyFetchBundle:
    pro = get_thread_pro_client(cfg)

    if "fact_daily_bar" in missing_tables:
        current.daily_df = safe_call(
            pro.daily,
            retries=cfg.retries,
            sleep_sec=cfg.sleep_sec,
            trade_date=trade_date,
        )
        time.sleep(cfg.sleep_sec)

    if "fact_adj_factor" in missing_tables:
        current.adj_df = safe_call(
            pro.adj_factor,
            retries=cfg.retries,
            sleep_sec=cfg.sleep_sec,
            trade_date=trade_date,
        )
        time.sleep(cfg.sleep_sec)

    if "fact_daily_basic" in missing_tables:
        current.basic_df = safe_call(
            pro.daily_basic,
            retries=cfg.retries,
            sleep_sec=cfg.sleep_sec,
            trade_date=trade_date,
        )
        time.sleep(cfg.sleep_sec)

    if "fact_stk_limit" in missing_tables:
        current.limit_df = safe_call(
            pro.stk_limit,
            retries=cfg.retries,
            sleep_sec=cfg.sleep_sec,
            trade_date=trade_date,
        )
        time.sleep(cfg.sleep_sec)

    return current


def empty_daily_bundle(trade_date: str) -> DailyFetchBundle:
    empty = pd.DataFrame()
    return DailyFetchBundle(
        trade_date=trade_date,
        daily_df=empty,
        adj_df=empty,
        basic_df=empty,
        limit_df=empty,
        suspend_df=empty,
    )


def repair_unresolved_dates(
    conn: sqlite3.Connection,
    cfg: Config,
    stock_basic_df: pd.DataFrame,
    unresolved_dates: list[tuple[str, list[str]]],
) -> list[tuple[str, list[str]]]:
    rounds = max(0, int(cfg.unresolved_rounds))
    if rounds <= 0 or not unresolved_dates:
        return unresolved_dates

    remaining: dict[str, list[str]] = {}
    for d, missing in unresolved_dates:
        remaining[d] = list(missing)

    for round_idx in range(1, rounds + 1):
        if not remaining:
            break

        LOGGER.info("Post-pass repair round %s/%s: dates=%s", round_idx, rounds, len(remaining))
        print(f"Post-pass repair round {round_idx}/{rounds}: dates={len(remaining)}")
        next_remaining: dict[str, list[str]] = {}
        repaired = 0

        for i, d in enumerate(sorted(remaining.keys()), start=1):
            counts_before = get_trade_date_counts(conn, d)
            missing_now = missing_tables_from_counts(counts_before)
            if not missing_now:
                repaired += 1
                continue

            try:
                bundle = fetch_missing_components(d, cfg, missing_now, empty_daily_bundle(d))
                _, retries_used, missing_after, _ = write_with_integrity_retry(conn, cfg, d, bundle, stock_basic_df)
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.warning("Post-pass repair failed for %s: %s", d, exc)
                missing_after = missing_now
                retries_used = 0

            if missing_after:
                next_remaining[d] = missing_after
            else:
                repaired += 1

            LOGGER.info(
                "Post-pass (%s/%s) %s | retries=%s | missing=%s",
                i,
                len(remaining),
                d,
                retries_used,
                ",".join(missing_after) if missing_after else "none",
            )
            if cfg.unresolved_cooldown_sec > 0:
                time.sleep(cfg.unresolved_cooldown_sec)

        remaining = next_remaining
        LOGGER.info(
            "Post-pass round %s done: repaired=%s, still_missing=%s",
            round_idx,
            repaired,
            len(remaining),
        )
        print(f"Post-pass round {round_idx} done: repaired={repaired}, still_missing={len(remaining)}")

    return [(d, m) for d, m in sorted(remaining.items())]


def write_unresolved_report(unresolved_dates: list[tuple[str, list[str]]], log_file: str) -> Optional[Path]:
    if not unresolved_dates:
        return None
    base_dir = Path(log_file).resolve().parent
    base_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = base_dir / f"unresolved_dates_{stamp}.csv"
    rows = [{"trade_date": d, "missing_tables": ",".join(m)} for d, m in unresolved_dates]
    pd.DataFrame(rows).to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path




def write_with_integrity_retry(
    conn: sqlite3.Connection,
    cfg: Config,
    trade_date: str,
    bundle: DailyFetchBundle,
    stock_basic_df: pd.DataFrame,
) -> tuple[dict[str, int], int, list[str], bool]:
    retries_used = 0
    current = bundle
    had_missing_once = False

    while True:
        write_daily_bundle(conn, cfg, current, stock_basic_df)
        counts = get_trade_date_counts(conn, trade_date)
        missing = missing_tables_from_counts(counts)

        if not missing:
            return counts, retries_used, missing, had_missing_once

        had_missing_once = True

        if retries_used >= max(0, int(cfg.per_date_retries)):
            return counts, retries_used, missing, had_missing_once

        retries_used += 1
        LOGGER.warning(
            "Integrity check failed for %s (missing=%s), retry %s/%s",
            trade_date,
            ",".join(missing),
            retries_used,
            cfg.per_date_retries,
        )
        current = fetch_missing_components(trade_date, cfg, missing, current)



def update_daily_tables(conn: sqlite3.Connection, cfg: Config, open_dates: Iterable[str], stock_basic_df: pd.DataFrame) -> None:
    all_dates = list(open_dates)
    if not all_dates:
        return

    skipped_complete = 0
    skipped_all_empty = 0
    precheck_missing_map: dict[str, list[str]] = {}

    if cfg.precheck_existing:
        precheck_missing_map, skipped_complete, skipped_all_empty = build_missing_map_for_dates(
            conn,
            all_dates,
            repair_only_missing=cfg.repair_only_missing,
        )
        dates = [d for d in all_dates if d in precheck_missing_map]
    else:
        dates = all_dates

    if not dates:
        print(f"Precheck: no dates need fetch. complete={skipped_complete}, all_empty_skipped={skipped_all_empty}")
        LOGGER.info("Precheck complete: all dates are complete, skipping pull.")
        return

    workers = max(1, int(cfg.workers))
    max_inflight = max(workers, int(cfg.max_inflight))
    inflight_cap = max_inflight
    min_inflight = max(1, min(int(cfg.adaptive_min_inflight), max_inflight))
    total = len(dates)

    LOGGER.info(
        "Parallel daily pull enabled: workers=%s, max_inflight=%s, total_dates=%s",
        workers,
        max_inflight,
        total,
    )
    progress = ConsoleProgress(total, "daily")
    progress.update(0, "starting", force=True)
    if cfg.precheck_existing:
        print(f"Precheck: skipped_complete={skipped_complete}, skipped_all_empty={skipped_all_empty}, to_fetch={total}")

    reset_api_stats()
    last_calls, last_errors, last_timeouts = snapshot_api_stats()
    last_eval_written = 0

    next_idx = 0
    written = 0
    future_to_date: dict[Future, str] = {}
    pending: set[Future] = set()
    recovered_dates: list[str] = []
    unresolved_dates: list[tuple[str, list[str]]] = []

    def submit_one(executor: ThreadPoolExecutor) -> bool:
        nonlocal next_idx
        if next_idx >= total:
            return False
        d = dates[next_idx]
        fut = executor.submit(fetch_daily_bundle, d, cfg)
        pending.add(fut)
        future_to_date[fut] = d
        next_idx += 1
        return True

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="ts-pull") as executor:
        while len(pending) < inflight_cap and submit_one(executor):
            pass

        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for fut in done:
                pending.remove(fut)
                d = future_to_date.pop(fut)

                try:
                    bundle = fut.result()
                except Exception as exc:  # pylint: disable=broad-except
                    LOGGER.warning("Fetch bundle failed for %s: %s", d, exc)
                    bundle = empty_daily_bundle(d)

                while len(pending) < inflight_cap and submit_one(executor):
                    pass

                counts, retries_used, missing, had_missing_once = write_with_integrity_retry(conn, cfg, d, bundle, stock_basic_df)
                written += 1
                suspend_count = get_table_count(conn, "fact_suspend_d", d)
                LOGGER.info(
                    "(%s/%s) Written %s | daily=%s adj=%s basic=%s limit=%s suspend=%s | retries=%s missing=%s",
                    written,
                    total,
                    d,
                    counts.get("fact_daily_bar", 0),
                    counts.get("fact_adj_factor", 0),
                    counts.get("fact_daily_basic", 0),
                    counts.get("fact_stk_limit", 0),
                    suspend_count,
                    retries_used,
                    ",".join(missing) if missing else "none",
                )

                if missing:
                    unresolved_dates.append((d, missing))
                elif had_missing_once:
                    recovered_dates.append(d)

                if missing:
                    progress.update(written, f"{d} retry={retries_used} miss={','.join(missing)}")
                elif had_missing_once:
                    progress.update(written, f"{d} recovered retry={retries_used}")
                else:
                    progress.update(written, d)

                if cfg.adaptive_concurrency and (written - last_eval_written) >= max(1, int(cfg.adaptive_window)):
                    calls, errors, timeouts = snapshot_api_stats()
                    delta_calls = max(0, calls - last_calls)
                    delta_errors = max(0, errors - last_errors)
                    delta_timeouts = max(0, timeouts - last_timeouts)
                    error_rate = (delta_errors / delta_calls) if delta_calls > 0 else 0.0
                    timeout_ratio = (delta_timeouts / delta_errors) if delta_errors > 0 else 0.0
                    prev_cap = inflight_cap

                    if (
                        error_rate >= float(cfg.adaptive_high_error_rate)
                        or timeout_ratio >= float(cfg.adaptive_high_timeout_ratio)
                    ):
                        reduced = int(inflight_cap * float(cfg.adaptive_reduce_factor))
                        if reduced >= inflight_cap:
                            reduced = inflight_cap - 1
                        inflight_cap = max(min_inflight, reduced)
                    elif (
                        error_rate <= float(cfg.adaptive_low_error_rate)
                        and inflight_cap < max_inflight
                    ):
                        inflight_cap = min(
                            max_inflight,
                            inflight_cap + max(1, int(cfg.adaptive_increase_step)),
                        )

                    if inflight_cap != prev_cap:
                        LOGGER.info(
                            "Adaptive inflight: %s -> %s | window_calls=%s errors=%s timeouts=%s err_rate=%.2f timeout_ratio=%.2f",
                            prev_cap,
                            inflight_cap,
                            delta_calls,
                            delta_errors,
                            delta_timeouts,
                            error_rate,
                            timeout_ratio,
                        )

                    last_calls, last_errors, last_timeouts = calls, errors, timeouts
                    last_eval_written = written

    progress.close("done")
    unresolved_dates = repair_unresolved_dates(conn, cfg, stock_basic_df, unresolved_dates)
    report_path = write_unresolved_report(unresolved_dates, cfg.log_file)
    print(f"Integrity summary: recovered={len(recovered_dates)}, unresolved={len(unresolved_dates)}, skipped_complete={skipped_complete}, skipped_all_empty={skipped_all_empty}")
    if unresolved_dates:
        preview = "; ".join([f"{d}:{','.join(m)}" for d, m in unresolved_dates[:20]])
        print(f"Unresolved sample: {preview}")
        if report_path is not None:
            print(f"Unresolved report: {report_path}")

def compute_date_range_for_update(conn: sqlite3.Connection, end_date: str, lookback_days: int) -> tuple[str, str]:
    latest = max_trade_date(conn, "fact_daily_bar")
    if latest is None:
        start = (dt.datetime.strptime(end_date, "%Y%m%d") - dt.timedelta(days=lookback_days)).strftime("%Y%m%d")
        return start, end_date
    start_dt = dt.datetime.strptime(latest, "%Y%m%d") - dt.timedelta(days=lookback_days)
    return start_dt.strftime("%Y%m%d"), end_date


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build local daily A-share DB with Tushare Pro")
    mode = p.add_mutually_exclusive_group(required=False)
    mode.add_argument("--init", action="store_true", help="Full initialization from start-date to end-date")
    mode.add_argument("--update", action="store_true", help="Incremental update from last trade_date")

    default_end = today_yyyymmdd() if USER_DEFAULTS.get("end_date") == "AUTO_TODAY" else str(USER_DEFAULTS.get("end_date"))

    p.add_argument("--token", default=os.getenv("TUSHARE_TOKEN", ""), help="Tushare token (or env TUSHARE_TOKEN)")
    p.add_argument("--http-url", default=os.getenv("TUSHARE_HTTP_URL", ""), help="Optional Tushare HTTP URL override")
    p.add_argument(
        "--secret-file",
        default=USER_DEFAULTS.get("secret_file", "secrets/tushare.env"),
        help="Optional local secret file containing TUSHARE_TOKEN/TUSHARE_HTTP_URL",
    )
    p.add_argument("--db-path", default=USER_DEFAULTS.get("db_path", "data/daily_bars/a_share_daily.db"), help="Local SQLite path")
    p.add_argument("--log-file", default=USER_DEFAULTS.get("log_file", "logs/build_a_share_daily_db.log"), help="Log file path")
    p.add_argument("--start-date", default=str(USER_DEFAULTS.get("start_date", "20100101")), help="YYYYMMDD")
    p.add_argument("--end-date", default=default_end, help="YYYYMMDD")
    p.add_argument("--lookback-days", type=int, default=int(USER_DEFAULTS.get("lookback_days", 10)), help="Extra backfill days for --update")
    p.add_argument("--min-listed-days", type=int, default=int(USER_DEFAULTS.get("min_listed_days", 120)), help="Universe filter")
    p.add_argument("--min-amount", type=float, default=float(USER_DEFAULTS.get("min_amount", 20000000.0)), help="Universe filter: minimum amount")
    p.add_argument("--min-price", type=float, default=float(USER_DEFAULTS.get("min_price", 3.0)), help="Universe filter: minimum close price")
    p.add_argument("--retries", type=int, default=int(USER_DEFAULTS.get("retries", 3)), help="API retry times")
    p.add_argument("--sleep-sec", type=float, default=float(USER_DEFAULTS.get("sleep_sec", 0.2)), help="Sleep seconds between API calls")
    p.add_argument("--timeout-sec", type=int, default=int(USER_DEFAULTS.get("timeout_sec", 60)), help="HTTP timeout seconds for each API request")
    p.add_argument("--namechange-chunk-days", type=int, default=int(USER_DEFAULTS.get("namechange_chunk_days", 180)), help="Chunk size in days for namechange pulling")
    p.add_argument(
        "--skip-namechange",
        action=argparse.BooleanOptionalAction,
        default=bool(USER_DEFAULTS.get("skip_namechange", False)),
        help="Skip refreshing fact_namechange",
    )
    p.add_argument("--workers", type=int, default=int(USER_DEFAULTS.get("workers", 6)), help="Parallel workers for daily API pulling")
    p.add_argument("--max-inflight", type=int, default=int(USER_DEFAULTS.get("max_inflight", 18)), help="Maximum in-flight trade dates during parallel pulling")
    p.add_argument("--verbose", action=argparse.BooleanOptionalAction, default=bool(USER_DEFAULTS.get("verbose", False)), help="Verbose logs")
    p.add_argument("--per-date-retries", type=int, default=int(USER_DEFAULTS.get("per_date_retries", 2)), help="Retries for one trade_date when integrity check fails")
    p.add_argument("--precheck-existing", action=argparse.BooleanOptionalAction, default=bool(USER_DEFAULTS.get("precheck_existing", True)), help="Precheck existing data and skip complete dates")
    p.add_argument("--repair-only-missing", action=argparse.BooleanOptionalAction, default=bool(USER_DEFAULTS.get("repair_only_missing", False)), help="Only repair partially-missing dates; skip all-empty dates")
    p.add_argument("--unresolved-rounds", type=int, default=int(USER_DEFAULTS.get("unresolved_rounds", 2)), help="Extra post-pass repair rounds for unresolved dates")
    p.add_argument("--unresolved-cooldown-sec", type=float, default=float(USER_DEFAULTS.get("unresolved_cooldown_sec", 1.0)), help="Sleep seconds between unresolved-date repairs")
    p.add_argument("--adaptive-concurrency", action=argparse.BooleanOptionalAction, default=bool(USER_DEFAULTS.get("adaptive_concurrency", True)), help="Auto-tune in-flight concurrency by API quality")
    p.add_argument("--adaptive-window", type=int, default=int(USER_DEFAULTS.get("adaptive_window", 40)), help="Evaluate adaptive concurrency every N written dates")
    p.add_argument("--adaptive-min-inflight", type=int, default=int(USER_DEFAULTS.get("adaptive_min_inflight", 8)), help="Minimum in-flight cap for adaptive mode")
    p.add_argument("--adaptive-reduce-factor", type=float, default=float(USER_DEFAULTS.get("adaptive_reduce_factor", 0.75)), help="Downshift factor when API quality degrades")
    p.add_argument("--adaptive-increase-step", type=int, default=int(USER_DEFAULTS.get("adaptive_increase_step", 2)), help="Upshift step when API quality recovers")
    p.add_argument("--adaptive-high-error-rate", type=float, default=float(USER_DEFAULTS.get("adaptive_high_error_rate", 0.35)), help="Downshift threshold for error attempts / calls")
    p.add_argument("--adaptive-low-error-rate", type=float, default=float(USER_DEFAULTS.get("adaptive_low_error_rate", 0.08)), help="Upshift threshold for error attempts / calls")
    p.add_argument("--adaptive-high-timeout-ratio", type=float, default=float(USER_DEFAULTS.get("adaptive_high_timeout_ratio", 0.60)), help="Downshift threshold for timeout errors / all errors")

    args = p.parse_args()

    if not args.init and not args.update:
        mode_default = str(USER_DEFAULTS.get("mode", "update")).strip().lower()
        if mode_default == "init":
            args.init = True
        else:
            args.update = True

    return args


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose, args.log_file)

    secret_cfg = load_simple_env(Path(args.secret_file))
    token = args.token.strip() or secret_cfg.get("TUSHARE_TOKEN", "").strip()
    http_url = args.http_url.strip() or secret_cfg.get("TUSHARE_HTTP_URL", "").strip()

    if not token:
        raise SystemExit("Missing Tushare token. Use --token or set TUSHARE_TOKEN.")

    db_path = Path(args.db_path).resolve()
    ensure_parent(db_path)

    cfg = Config(
        token=token,
        http_url=http_url or None,
        db_path=db_path,
        log_file=args.log_file,
        start_date=args.start_date,
        end_date=args.end_date,
        min_listed_days=args.min_listed_days,
        min_amount=args.min_amount,
        min_price=args.min_price,
        retries=args.retries,
        sleep_sec=args.sleep_sec,
        workers=args.workers,
        max_inflight=args.max_inflight,
        timeout_sec=args.timeout_sec,
        namechange_chunk_days=args.namechange_chunk_days,
        skip_namechange=args.skip_namechange,
        per_date_retries=args.per_date_retries,
        precheck_existing=args.precheck_existing,
        repair_only_missing=args.repair_only_missing,
        unresolved_rounds=args.unresolved_rounds,
        unresolved_cooldown_sec=args.unresolved_cooldown_sec,
        adaptive_concurrency=args.adaptive_concurrency,
        adaptive_window=args.adaptive_window,
        adaptive_min_inflight=args.adaptive_min_inflight,
        adaptive_reduce_factor=args.adaptive_reduce_factor,
        adaptive_increase_step=args.adaptive_increase_step,
        adaptive_high_error_rate=args.adaptive_high_error_rate,
        adaptive_low_error_rate=args.adaptive_low_error_rate,
        adaptive_high_timeout_ratio=args.adaptive_high_timeout_ratio,
    )

    pro = create_pro_client(cfg.token, cfg.http_url, cfg.timeout_sec)

    conn = sqlite3.connect(str(cfg.db_path))
    try:
        create_schema(conn)

        if args.update:
            start_date, end_date = compute_date_range_for_update(conn, cfg.end_date, args.lookback_days)
            cfg.start_date = start_date
            cfg.end_date = end_date
            LOGGER.info("Update mode date range: %s -> %s", start_date, end_date)
        else:
            LOGGER.info("Init mode date range: %s -> %s", cfg.start_date, cfg.end_date)

        stock_basic_df = update_static_tables(conn, pro, cfg)
        open_dates = get_open_dates(conn, cfg.start_date, cfg.end_date)
        LOGGER.info("Open trading dates to process: %s", len(open_dates))

        if not open_dates:
            LOGGER.warning("No open trading dates found in range.")
            return

        update_daily_tables(conn, cfg, open_dates, stock_basic_df)
        LOGGER.info("Done. DB saved at: %s", cfg.db_path)
        print(f"Done. DB saved at: {cfg.db_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()











