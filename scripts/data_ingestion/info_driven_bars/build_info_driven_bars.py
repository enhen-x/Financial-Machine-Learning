#!/usr/bin/env python
"""
Build/update local info-driven bars (tick/volume/dollar) from Tushare minute data.

Usage examples:
  python scripts/data_ingestion/info_driven_bars/build_info_driven_bars.py --init
  python scripts/data_ingestion/info_driven_bars/build_info_driven_bars.py --update
  python scripts/data_ingestion/info_driven_bars/build_info_driven_bars.py --update --lookback-days 10
  python scripts/data_ingestion/info_driven_bars/build_info_driven_bars.py --init --start-date 20250101 --end-date 20250331 --bar-types tick,volume,dollar
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
from typing import Iterable, Optional

import pandas as pd
import tushare as ts


LOGGER = logging.getLogger("info_driven_bars")


USER_DEFAULTS = {
    "mode": "init",  # init|update
    "secret_file": "secrets/tushare.env",  # token/http url file
    "source_db_path": "data/daily_bars/a_share_daily.db",  # source daily database
    "target_db_path": "data/info_driven_bars/a_share_info_driven.db",  # target info-driven database
    "log_file": "logs/build_info_driven_bars.log",  # log file path
    "start_date": "20200101",  # YYYYMMDD
    "end_date": "AUTO_TODAY",  # AUTO_TODAY or YYYYMMDD
    "lookback_days": 10,  # for update mode
    "ts_codes": "",  # comma separated ts_codes; empty means auto from source_db
    "max_symbols_per_day": 200,  # cap symbols per day for ingestion stability
    "include_st": False,  # include ST symbols when auto selecting from source_db
    "bar_types": "tick,volume,dollar",  # supported: tick,volume,dollar
    "tick_threshold": 30,  # rows per tick bar
    "volume_threshold": 300000.0,  # shares per volume bar
    "dollar_threshold": 50000000.0,  # amount per dollar bar
    "tail_partial": True,  # emit last partial bar for each day
    "minute_freq": "1min",  # minute source frequency passed to tushare endpoint
    "workers": 8,  # parallel workers for fetch+transform
    "max_inflight": 16,  # max in-flight symbol-day tasks
    "retries": 3,  # API retries in safe_call
    "sleep_sec": 0.2,  # backoff base + throttle sleep
    "timeout_sec": 30,  # HTTP timeout
    "api_calls_per_minute": 2,  # Global API rate limit for minute endpoint (2/min for current permission)
    "http_proxy": "",  # Optional outbound HTTP proxy, e.g. http://127.0.0.1:7890
    "https_proxy": "",  # Optional outbound HTTPS proxy, e.g. http://127.0.0.1:7890
    "no_proxy": "",  # Optional NO_PROXY, e.g. 127.0.0.1,localhost
}


@dataclass
class Config:
    token: str
    http_url: Optional[str]
    source_db_path: Path
    target_db_path: Path
    log_file: str
    start_date: str
    end_date: str
    lookback_days: int
    ts_codes: list[str]
    max_symbols_per_day: int
    include_st: bool
    bar_types: list[str]
    tick_threshold: int
    volume_threshold: float
    dollar_threshold: float
    tail_partial: bool
    minute_freq: str
    workers: int
    max_inflight: int
    retries: int
    sleep_sec: float
    timeout_sec: int
    api_calls_per_minute: int
    http_proxy: Optional[str]
    https_proxy: Optional[str]
    no_proxy: Optional[str]


@dataclass
class TaskResult:
    trade_date: str
    ts_code: str
    bars_by_type: dict[str, pd.DataFrame]
    error: Optional[str]


_THREAD_LOCAL = threading.local()
_API_GATE_LOCK = threading.Lock()
_API_NEXT_TS = 0.0


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
        if not force and self.current < self.total and (now - self.last_render_ts) < 0.3:
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


def setup_logging(log_file: str) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)

    p = Path(log_file).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)

    h = logging.FileHandler(p, encoding="utf-8")
    h.setLevel(logging.INFO)
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"))
    root.addHandler(h)

def apply_proxy_env(http_proxy: Optional[str], https_proxy: Optional[str], no_proxy: Optional[str]) -> None:
    if http_proxy:
        os.environ["HTTP_PROXY"] = http_proxy
        os.environ["http_proxy"] = http_proxy
    if https_proxy:
        os.environ["HTTPS_PROXY"] = https_proxy
        os.environ["https_proxy"] = https_proxy
    if no_proxy:
        os.environ["NO_PROXY"] = no_proxy
        os.environ["no_proxy"] = no_proxy


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
        val = v.strip().strip('"').strip("'")
        if key:
            out[key] = val
    return out


def create_pro_client(token: str, http_url: Optional[str], timeout_sec: int) -> ts.pro_api:
    pro = ts.pro_api(token)
    if http_url:
        pro._DataApi__http_url = http_url  # pylint: disable=protected-access
    pro._DataApi__timeout = int(timeout_sec)  # pylint: disable=protected-access
    return pro


def get_thread_pro_client(cfg: Config):
    pro = getattr(_THREAD_LOCAL, "pro", None)
    if pro is None:
        pro = create_pro_client(cfg.token, cfg.http_url, cfg.timeout_sec)
        _THREAD_LOCAL.pro = pro
    return pro


def throttle_api_call(calls_per_minute: int) -> None:
    if calls_per_minute <= 0:
        return
    interval = 60.0 / float(calls_per_minute)
    global _API_NEXT_TS
    wait_sec = 0.0
    with _API_GATE_LOCK:
        now = time.monotonic()
        target = max(now, _API_NEXT_TS)
        _API_NEXT_TS = target + interval
        wait_sec = target - now
    if wait_sec > 0:
        time.sleep(wait_sec)


def is_rate_limited_error(exc: Exception) -> bool:
    msg = str(exc)
    keys = (
        "??????????",
        "???????",
        "rate limit",
        "too many requests",
    )
    lower_msg = msg.lower()
    return any((k in msg) or (k in lower_msg) for k in keys)


def safe_call(func, retries: int, sleep_sec: float, calls_per_minute: int = 0, **kwargs) -> pd.DataFrame:
    last_err = None
    for i in range(retries):
        throttle_api_call(calls_per_minute)
        try:
            df = func(**kwargs)
            return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
        except Exception as exc:  # pylint: disable=broad-except
            last_err = exc
            delay = sleep_sec * (i + 1)
            if is_rate_limited_error(exc):
                delay = max(delay, 60.0 / float(max(1, calls_per_minute or 2)))
            LOGGER.warning("API call failed (%s), retrying in %.1fs ...", exc, delay)
            time.sleep(delay)
    raise RuntimeError(f"API call failed after {retries} retries: {last_err}") from last_err


def create_target_schema(conn: sqlite3.Connection) -> None:
    ddl = """
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS fact_info_driven_bar (
        ts_code TEXT,
        trade_date TEXT,
        bar_type TEXT,
        bar_index INTEGER,
        start_time TEXT,
        end_time TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        amount REAL,
        n_ticks INTEGER,
        cum_metric REAL,
        source_freq TEXT,
        created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
        PRIMARY KEY (ts_code, trade_date, bar_type, bar_index)
    );

    CREATE INDEX IF NOT EXISTS idx_info_bar_date ON fact_info_driven_bar(trade_date);
    CREATE INDEX IF NOT EXISTS idx_info_bar_code_date ON fact_info_driven_bar(ts_code, trade_date);
    CREATE INDEX IF NOT EXISTS idx_info_bar_type_date ON fact_info_driven_bar(bar_type, trade_date);
    """
    conn.executescript(ddl)
    conn.commit()


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def upsert_df(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    tmp = f"_tmp_{table}_{int(time.time() * 1000)}"
    df.to_sql(tmp, conn, if_exists="replace", index=False)
    cols = list(df.columns)
    col_csv = ", ".join(quote_ident(c) for c in cols)
    conn.execute(
        f"INSERT OR REPLACE INTO {quote_ident(table)} ({col_csv}) SELECT {col_csv} FROM {quote_ident(tmp)}"
    )
    conn.execute(f"DROP TABLE {quote_ident(tmp)}")
    conn.commit()
    return len(df)


def compute_date_range_for_update(conn: sqlite3.Connection, end_date: str, lookback_days: int) -> tuple[str, str]:
    row = conn.execute("SELECT MAX(trade_date) FROM fact_info_driven_bar").fetchone()
    latest = row[0] if row and row[0] else None
    if latest is None:
        start = (dt.datetime.strptime(end_date, "%Y%m%d") - dt.timedelta(days=lookback_days)).strftime("%Y%m%d")
        return start, end_date
    start_dt = dt.datetime.strptime(latest, "%Y%m%d") - dt.timedelta(days=lookback_days)
    return start_dt.strftime("%Y%m%d"), end_date


def get_open_dates(conn: sqlite3.Connection, start_date: str, end_date: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT cal_date
        FROM fact_trade_cal
        WHERE exchange='SSE' AND is_open=1 AND cal_date BETWEEN ? AND ?
        ORDER BY cal_date
        """,
        (start_date, end_date),
    ).fetchall()
    return [r[0] for r in rows]


def get_symbols_for_date(conn: sqlite3.Connection, trade_date: str, max_symbols: int, include_st: bool) -> list[str]:
    if include_st:
        rows = conn.execute(
            """
            SELECT ts_code
            FROM fact_universe_pti
            WHERE trade_date=? AND in_universe=1
            ORDER BY ts_code
            LIMIT ?
            """,
            (trade_date, max_symbols),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT ts_code
            FROM fact_universe_pti
            WHERE trade_date=? AND in_universe=1 AND is_st=0
            ORDER BY ts_code
            LIMIT ?
            """,
            (trade_date, max_symbols),
        ).fetchall()

    out = [r[0] for r in rows]
    if out:
        return out

    fallback = conn.execute(
        """
        SELECT ts_code
        FROM dim_security
        WHERE list_status='L'
        ORDER BY ts_code
        LIMIT ?
        """,
        (max_symbols,),
    ).fetchall()
    return [r[0] for r in fallback]


def parse_bar_types(txt: str) -> list[str]:
    raw = [x.strip().lower() for x in txt.split(",") if x.strip()]
    allowed = {"tick", "volume", "dollar"}
    out = [x for x in raw if x in allowed]
    if not out:
        raise ValueError("bar_types must include at least one of: tick,volume,dollar")
    # keep order and dedupe
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def normalize_minute_df(df: pd.DataFrame, ts_code: str, trade_date: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts_code", "trade_date", "trade_time", "open", "high", "low", "close", "vol", "amount"])

    x = df.copy()
    cols_lower = {c.lower(): c for c in x.columns}

    def pick(*cands: str) -> Optional[str]:
        for c in cands:
            if c in cols_lower:
                return cols_lower[c]
        return None

    c_time = pick("trade_time", "datetime", "time")
    c_open = pick("open")
    c_high = pick("high")
    c_low = pick("low")
    c_close = pick("close", "price")
    c_vol = pick("vol", "volume")
    c_amount = pick("amount")

    if c_close is None or c_time is None:
        return pd.DataFrame(columns=["ts_code", "trade_date", "trade_time", "open", "high", "low", "close", "vol", "amount"])

    out = pd.DataFrame()
    out["ts_code"] = x[pick("ts_code")].astype(str) if pick("ts_code") else ts_code
    out["trade_date"] = x[pick("trade_date")].astype(str) if pick("trade_date") else trade_date
    out["trade_time"] = x[c_time].astype(str)

    out["close"] = pd.to_numeric(x[c_close], errors="coerce")
    out["open"] = pd.to_numeric(x[c_open], errors="coerce") if c_open else out["close"]
    out["high"] = pd.to_numeric(x[c_high], errors="coerce") if c_high else out["close"]
    out["low"] = pd.to_numeric(x[c_low], errors="coerce") if c_low else out["close"]
    out["vol"] = pd.to_numeric(x[c_vol], errors="coerce").fillna(0.0) if c_vol else 0.0
    out["amount"] = pd.to_numeric(x[c_amount], errors="coerce").fillna(0.0) if c_amount else 0.0

    out = out.dropna(subset=["close", "open", "high", "low"])
    out = out.sort_values("trade_time").reset_index(drop=True)
    return out


def build_info_driven_bars(
    minute_df: pd.DataFrame,
    bar_type: str,
    threshold: float,
    tail_partial: bool,
    source_freq: str,
) -> pd.DataFrame:
    if minute_df.empty:
        return pd.DataFrame()
    if threshold <= 0:
        raise ValueError(f"threshold must be > 0 for {bar_type}")

    out_rows = []

    n_ticks = 0
    cum_metric = 0.0
    cum_volume = 0.0
    cum_amount = 0.0
    o = h = l = c = None
    start_t = end_t = None
    bar_index = 0

    def emit():
        nonlocal bar_index, n_ticks, cum_metric, cum_volume, cum_amount, o, h, l, c, start_t, end_t
        if n_ticks <= 0:
            return
        bar_index += 1
        out_rows.append(
            {
                "ts_code": minute_df.iloc[0]["ts_code"],
                "trade_date": minute_df.iloc[0]["trade_date"],
                "bar_type": bar_type,
                "bar_index": bar_index,
                "start_time": str(start_t),
                "end_time": str(end_t),
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "volume": float(cum_volume),
                "amount": float(cum_amount),
                "n_ticks": int(n_ticks),
                "cum_metric": float(cum_metric),
                "source_freq": source_freq,
            }
        )
        n_ticks = 0
        cum_metric = 0.0
        cum_volume = 0.0
        cum_amount = 0.0
        o = h = l = c = None
        start_t = end_t = None

    for _, r in minute_df.iterrows():
        px = float(r["close"])
        vol = float(r["vol"])
        amt = float(r["amount"])
        t = r["trade_time"]

        if n_ticks == 0:
            o = px
            h = px
            l = px
            start_t = t
        else:
            h = max(float(h), px)
            l = min(float(l), px)

        c = px
        end_t = t
        n_ticks += 1
        cum_volume += vol
        cum_amount += amt

        if bar_type == "tick":
            metric = 1.0
        elif bar_type == "volume":
            metric = max(0.0, vol)
        elif bar_type == "dollar":
            metric = max(0.0, amt)
        else:
            raise ValueError(f"unsupported bar_type: {bar_type}")

        cum_metric += metric
        if cum_metric >= threshold:
            emit()

    if tail_partial and n_ticks > 0:
        emit()

    if not out_rows:
        return pd.DataFrame()
    return pd.DataFrame(out_rows)


def fetch_minute_df(ts_code: str, trade_date: str, cfg: Config) -> pd.DataFrame:
    pro = get_thread_pro_client(cfg)
    df = safe_call(
        pro.stk_mins,
        retries=cfg.retries,
        sleep_sec=cfg.sleep_sec,
        calls_per_minute=cfg.api_calls_per_minute,
        ts_code=ts_code,
        trade_date=trade_date,
        freq=cfg.minute_freq,
    )
    if cfg.sleep_sec > 0:
        time.sleep(cfg.sleep_sec)
    return normalize_minute_df(df, ts_code, trade_date)


def run_one_task(ts_code: str, trade_date: str, cfg: Config) -> TaskResult:
    try:
        minute_df = fetch_minute_df(ts_code, trade_date, cfg)
        if minute_df.empty:
            return TaskResult(trade_date=trade_date, ts_code=ts_code, bars_by_type={}, error=None)

        bars_by_type: dict[str, pd.DataFrame] = {}
        for bt in cfg.bar_types:
            if bt == "tick":
                th = float(cfg.tick_threshold)
            elif bt == "volume":
                th = float(cfg.volume_threshold)
            else:
                th = float(cfg.dollar_threshold)
            bars_by_type[bt] = build_info_driven_bars(minute_df, bt, th, cfg.tail_partial, cfg.minute_freq)

        return TaskResult(trade_date=trade_date, ts_code=ts_code, bars_by_type=bars_by_type, error=None)
    except Exception as exc:  # pylint: disable=broad-except
        return TaskResult(trade_date=trade_date, ts_code=ts_code, bars_by_type={}, error=str(exc))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build info-driven bars from Tushare minute data")
    mode = p.add_mutually_exclusive_group(required=False)
    mode.add_argument("--init", action="store_true", help="Full pull from start-date to end-date")
    mode.add_argument("--update", action="store_true", help="Incremental update from target db latest trade_date")

    default_end = today_yyyymmdd() if USER_DEFAULTS.get("end_date") == "AUTO_TODAY" else str(USER_DEFAULTS.get("end_date"))

    p.add_argument("--token", default=os.getenv("TUSHARE_TOKEN", ""), help="Tushare token")
    p.add_argument("--http-url", default=os.getenv("TUSHARE_HTTP_URL", ""), help="Optional Tushare HTTP URL override")
    p.add_argument("--secret-file", default=str(USER_DEFAULTS.get("secret_file", "secrets/tushare.env")), help="Secret file path")
    p.add_argument("--source-db-path", default=str(USER_DEFAULTS.get("source_db_path", "data/daily_bars/a_share_daily.db")), help="Source daily db path")
    p.add_argument("--target-db-path", default=str(USER_DEFAULTS.get("target_db_path", "data/info_driven_bars/a_share_info_driven.db")), help="Target info-driven db path")
    p.add_argument("--log-file", default=str(USER_DEFAULTS.get("log_file", "logs/build_info_driven_bars.log")), help="Log file path")
    p.add_argument("--start-date", default=str(USER_DEFAULTS.get("start_date", "20200101")), help="YYYYMMDD")
    p.add_argument("--end-date", default=default_end, help="YYYYMMDD")
    p.add_argument("--lookback-days", type=int, default=int(USER_DEFAULTS.get("lookback_days", 10)), help="Lookback days for update")
    p.add_argument("--ts-codes", default=str(USER_DEFAULTS.get("ts_codes", "")), help="Comma separated ts_codes")
    p.add_argument("--max-symbols-per-day", type=int, default=int(USER_DEFAULTS.get("max_symbols_per_day", 200)), help="Max symbols per day when auto selecting")
    p.add_argument("--include-st", action=argparse.BooleanOptionalAction, default=bool(USER_DEFAULTS.get("include_st", False)), help="Include ST symbols in auto selection")
    p.add_argument("--bar-types", default=str(USER_DEFAULTS.get("bar_types", "tick,volume,dollar")), help="tick,volume,dollar")
    p.add_argument("--tick-threshold", type=int, default=int(USER_DEFAULTS.get("tick_threshold", 30)), help="Tick-bar threshold")
    p.add_argument("--volume-threshold", type=float, default=float(USER_DEFAULTS.get("volume_threshold", 300000.0)), help="Volume-bar threshold")
    p.add_argument("--dollar-threshold", type=float, default=float(USER_DEFAULTS.get("dollar_threshold", 50000000.0)), help="Dollar-bar threshold")
    p.add_argument("--tail-partial", action=argparse.BooleanOptionalAction, default=bool(USER_DEFAULTS.get("tail_partial", True)), help="Emit tail partial bars")
    p.add_argument("--minute-freq", default=str(USER_DEFAULTS.get("minute_freq", "1min")), help="Minute freq for source pull")
    p.add_argument("--workers", type=int, default=int(USER_DEFAULTS.get("workers", 8)), help="Parallel workers")
    p.add_argument("--max-inflight", type=int, default=int(USER_DEFAULTS.get("max_inflight", 16)), help="Max in-flight tasks")
    p.add_argument("--retries", type=int, default=int(USER_DEFAULTS.get("retries", 3)), help="API retries")
    p.add_argument("--sleep-sec", type=float, default=float(USER_DEFAULTS.get("sleep_sec", 0.2)), help="Retry backoff base sleep")
    p.add_argument("--timeout-sec", type=int, default=int(USER_DEFAULTS.get("timeout_sec", 30)), help="HTTP timeout sec")
    p.add_argument("--api-calls-per-minute", type=int, default=int(USER_DEFAULTS.get("api_calls_per_minute", 2)), help="Global API rate limit (calls/minute)")
    p.add_argument("--http-proxy", default=str(USER_DEFAULTS.get("http_proxy", "")), help="HTTP proxy URL")
    p.add_argument("--https-proxy", default=str(USER_DEFAULTS.get("https_proxy", "")), help="HTTPS proxy URL")
    p.add_argument("--no-proxy", default=str(USER_DEFAULTS.get("no_proxy", "")), help="NO_PROXY hosts")

    args = p.parse_args()

    if not args.init and not args.update:
        if str(USER_DEFAULTS.get("mode", "update")).lower() == "init":
            args.init = True
        else:
            args.update = True

    return args


def build_tasks(source_conn: sqlite3.Connection, cfg: Config) -> list[tuple[str, str]]:
    dates = get_open_dates(source_conn, cfg.start_date, cfg.end_date)
    tasks: list[tuple[str, str]] = []

    if cfg.ts_codes:
        for d in dates:
            for c in cfg.ts_codes:
                tasks.append((d, c))
        return tasks

    for d in dates:
        symbols = get_symbols_for_date(source_conn, d, cfg.max_symbols_per_day, cfg.include_st)
        for c in symbols:
            tasks.append((d, c))
    return tasks


def main() -> None:
    args = parse_args()
    setup_logging(args.log_file)

    secret_cfg = load_simple_env(Path(args.secret_file))
    token = args.token.strip() or secret_cfg.get("TUSHARE_TOKEN", "").strip()
    http_url = args.http_url.strip() or secret_cfg.get("TUSHARE_HTTP_URL", "").strip()
    http_proxy = (
        args.http_proxy.strip()
        or secret_cfg.get("HTTP_PROXY", "").strip()
        or secret_cfg.get("TUSHARE_HTTP_PROXY", "").strip()
    )
    https_proxy = (
        args.https_proxy.strip()
        or secret_cfg.get("HTTPS_PROXY", "").strip()
        or secret_cfg.get("TUSHARE_HTTPS_PROXY", "").strip()
    )
    no_proxy = (
        args.no_proxy.strip()
        or secret_cfg.get("NO_PROXY", "").strip()
        or secret_cfg.get("TUSHARE_NO_PROXY", "").strip()
    )
    if not token:
        raise SystemExit("Missing Tushare token. Use --token or set TUSHARE_TOKEN.")

    apply_proxy_env(
        http_proxy=http_proxy or None,
        https_proxy=https_proxy or None,
        no_proxy=no_proxy or None,
    )

    source_db = Path(args.source_db_path).resolve()
    target_db = Path(args.target_db_path).resolve()
    ensure_parent(target_db)

    ts_codes = [x.strip() for x in str(args.ts_codes).split(",") if x.strip()]

    cfg = Config(
        token=token,
        http_url=http_url or None,
        source_db_path=source_db,
        target_db_path=target_db,
        log_file=args.log_file,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_days=max(0, int(args.lookback_days)),
        ts_codes=ts_codes,
        max_symbols_per_day=max(1, int(args.max_symbols_per_day)),
        include_st=bool(args.include_st),
        bar_types=parse_bar_types(args.bar_types),
        tick_threshold=max(1, int(args.tick_threshold)),
        volume_threshold=max(1.0, float(args.volume_threshold)),
        dollar_threshold=max(1.0, float(args.dollar_threshold)),
        tail_partial=bool(args.tail_partial),
        minute_freq=str(args.minute_freq),
        workers=max(1, int(args.workers)),
        max_inflight=max(1, int(args.max_inflight)),
        retries=max(1, int(args.retries)),
        sleep_sec=max(0.0, float(args.sleep_sec)),
        timeout_sec=max(1, int(args.timeout_sec)),
        api_calls_per_minute=max(1, int(args.api_calls_per_minute)),
        http_proxy=http_proxy or None,
        https_proxy=https_proxy or None,
        no_proxy=no_proxy or None,
    )

    if cfg.api_calls_per_minute <= 2 and (cfg.workers > 1 or cfg.max_inflight > 1):
        LOGGER.info("Low API rate limit detected (%s/min), forcing workers=1 max_inflight=1", cfg.api_calls_per_minute)
        cfg.workers = 1
        cfg.max_inflight = 1

    source_conn = sqlite3.connect(str(cfg.source_db_path))
    target_conn = sqlite3.connect(str(cfg.target_db_path))
    try:
        create_target_schema(target_conn)

        if args.update:
            start_date, end_date = compute_date_range_for_update(target_conn, cfg.end_date, cfg.lookback_days)
            cfg.start_date = start_date
            cfg.end_date = end_date
            LOGGER.info("Update mode date range: %s -> %s", cfg.start_date, cfg.end_date)
        else:
            LOGGER.info("Init mode date range: %s -> %s", cfg.start_date, cfg.end_date)

        tasks = build_tasks(source_conn, cfg)
        total = len(tasks)
        if total <= 0:
            print("No tasks to run.")
            return

        LOGGER.info("Info-driven pull: tasks=%s workers=%s max_inflight=%s rate_limit=%s/min bar_types=%s", total, cfg.workers, cfg.max_inflight, cfg.api_calls_per_minute, ",".join(cfg.bar_types))
        progress = ConsoleProgress(total, "info-bars")
        progress.update(0, "starting", force=True)

        next_idx = 0
        done_cnt = 0
        ok_cnt = 0
        fail_cnt = 0
        written_rows = 0

        pending: set[Future] = set()
        future_to_task: dict[Future, tuple[str, str]] = {}

        def submit_one(executor: ThreadPoolExecutor) -> bool:
            nonlocal next_idx
            if next_idx >= total:
                return False
            trade_date, ts_code = tasks[next_idx]
            fut = executor.submit(run_one_task, ts_code, trade_date, cfg)
            pending.add(fut)
            future_to_task[fut] = (trade_date, ts_code)
            next_idx += 1
            return True

        inflight = max(cfg.workers, cfg.max_inflight)

        with ThreadPoolExecutor(max_workers=cfg.workers, thread_name_prefix="info-bars") as executor:
            while len(pending) < inflight and submit_one(executor):
                pass

            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for fut in done:
                    pending.remove(fut)
                    d, c = future_to_task.pop(fut)

                    while len(pending) < inflight and submit_one(executor):
                        pass

                    try:
                        res = fut.result()
                    except Exception as exc:  # pylint: disable=broad-except
                        fail_cnt += 1
                        done_cnt += 1
                        LOGGER.warning("Task failed unexpectedly %s %s: %s", d, c, exc)
                        progress.update(done_cnt, f"fail={fail_cnt}")
                        continue

                    if res.error:
                        fail_cnt += 1
                        LOGGER.warning("Task failed %s %s: %s", d, c, res.error)
                    else:
                        local_rows = 0
                        for bt, bdf in res.bars_by_type.items():
                            if bdf is None or bdf.empty:
                                continue
                            n = upsert_df(target_conn, "fact_info_driven_bar", bdf)
                            local_rows += n
                        if local_rows > 0:
                            ok_cnt += 1
                            written_rows += local_rows
                        else:
                            ok_cnt += 1

                    done_cnt += 1
                    progress.update(done_cnt, f"ok={ok_cnt} fail={fail_cnt} rows={written_rows}")

        progress.close("done")
        LOGGER.info("Done. tasks=%s ok=%s fail=%s rows=%s target_db=%s", total, ok_cnt, fail_cnt, written_rows, cfg.target_db_path)
        print(f"Done. tasks={total} ok={ok_cnt} fail={fail_cnt} rows={written_rows}")
        print(f"Target DB: {cfg.target_db_path}")

    finally:
        source_conn.close()
        target_conn.close()


if __name__ == "__main__":
    main()

