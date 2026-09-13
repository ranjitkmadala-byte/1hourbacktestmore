from __future__ import annotations

import json
import os
import time
import uuid
from collections import Counter, defaultdict
from datetime import date, datetime, time as dtime, timedelta
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import psycopg
import requests
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

IST = ZoneInfo("Asia/Kolkata")

TOKEN = (os.getenv("UPSTOX_ACCESS_TOKEN") or os.getenv("UPSTOX_TOKEN") or "").strip()
DATABASE_URL = (os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()

STUDY_START = date.fromisoformat(os.getenv("STUDY_START", "2026-08-26"))
STUDY_END = date.fromisoformat(os.getenv("STUDY_END", "2026-09-11"))
EXPECTED_EVENTS = int(os.getenv("EXPECTED_1015_EVENTS", "88"))

BREAKOUT_TIME = dtime(10, 15)
MARKET_CLOSE = dtime(15, 30)
FAIL_THRESHOLD_PCT = float(os.getenv("ACCEPTANCE_FAIL_THRESHOLD_PCT", "0.25"))
TARGET_PCT = float(os.getenv("ACCEPTANCE_TARGET_PCT", "1.0"))
STOP_PCT = float(os.getenv("ACCEPTANCE_STOP_PCT", "1.0"))
WINDOWS = (5, 15, 30)

HIST_V3 = "https://api.upstox.com/v3/historical-candle"
RUN_ID = os.getenv("ACCEPTANCE_RUN_ID", "").strip() or str(uuid.uuid4())


def log(msg: str) -> None:
    print(f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST | {msg}", flush=True)


def headers() -> dict[str, str]:
    return {"Accept": "application/json", "Authorization": f"Bearer {TOKEN}"}


def api_get(url: str, attempts: int = 5) -> dict[str, Any]:
    last_exc = None
    for attempt in range(attempts):
        try:
            r = requests.get(url, headers=headers(), timeout=60)
            if r.status_code == 429:
                wait = 2.0 * (attempt + 1)
                log(f"Rate limited; sleeping {wait:.1f}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last_exc = exc
            if attempt + 1 < attempts:
                time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"GET failed after {attempts} attempts: {url}: {last_exc}")


def historical_1m(key: str, from_date: date, to_date: date) -> list[list[Any]]:
    url = (
        f"{HIST_V3}/{quote(key, safe='')}/minutes/1/"
        f"{to_date.isoformat()}/{from_date.isoformat()}"
    )
    return api_get(url).get("data", {}).get("candles", []) or []


def parse_candles(rows: list[list[Any]]) -> list[dict[str, Any]]:
    out = []
    for c in rows:
        if len(c) < 5:
            continue
        try:
            ts = datetime.fromisoformat(str(c[0]).replace("Z", "+00:00")).astimezone(IST)
            out.append({
                "ts": ts,
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": int(float(c[5])) if len(c) > 5 and c[5] is not None else 0,
            })
        except Exception:
            continue
    out.sort(key=lambda x: x["ts"])
    return out


def load_1015_events() -> list[dict[str, Any]]:
    sql = """
    SELECT
        study_start, study_end, run_id AS source_run_id,
        trading_date, symbol, spot_instrument_key,
        breakout_time, breakout_candle_start,
        entry_price AS breakout_entry_price,
        strong_supply_low, strong_supply_high,
        breakout_pct,
        t1_0_s1_0_outcome AS original_t1_s1_outcome,
        max_favorable_pct AS original_mfe_pct,
        max_adverse_pct AS original_mae_pct
    FROM public.spot_supply_1h_backtest_events
    WHERE study_start = %s
      AND study_end = %s
      AND (breakout_time AT TIME ZONE 'Asia/Kolkata')::time = TIME '10:15'
    ORDER BY trading_date, symbol;
    """
    with psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (STUDY_START, STUDY_END))
            return [dict(r) for r in cur.fetchall()]


DDL = """
CREATE TABLE IF NOT EXISTS public.spot_supply_acceptance_backtest (
    study_start DATE NOT NULL,
    study_end DATE NOT NULL,
    run_id UUID NOT NULL,
    source_run_id UUID,
    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    spot_instrument_key TEXT NOT NULL,

    breakout_time TIMESTAMPTZ NOT NULL,
    breakout_entry_price NUMERIC NOT NULL,
    strong_supply_low NUMERIC,
    strong_supply_high NUMERIC,
    breakout_pct NUMERIC,

    original_t1_s1_outcome TEXT,
    original_mfe_pct NUMERIC,
    original_mae_pct NUMERIC,

    state_5m TEXT,
    close_5m NUMERIC,
    close_dist_supply_5m_pct NUMERIC,
    min_dist_supply_5m_pct NUMERIC,
    retested_5m BOOLEAN,
    entry_after_5m NUMERIC,
    outcome_after_5m TEXT,
    outcome_time_after_5m TIMESTAMPTZ,
    mfe_after_5m_pct NUMERIC,
    mae_after_5m_pct NUMERIC,

    state_15m TEXT,
    close_15m NUMERIC,
    close_dist_supply_15m_pct NUMERIC,
    min_dist_supply_15m_pct NUMERIC,
    retested_15m BOOLEAN,
    entry_after_15m NUMERIC,
    outcome_after_15m TEXT,
    outcome_time_after_15m TIMESTAMPTZ,
    mfe_after_15m_pct NUMERIC,
    mae_after_15m_pct NUMERIC,

    state_30m TEXT,
    close_30m NUMERIC,
    close_dist_supply_30m_pct NUMERIC,
    min_dist_supply_30m_pct NUMERIC,
    retested_30m BOOLEAN,
    entry_after_30m NUMERIC,
    outcome_after_30m TEXT,
    outcome_time_after_30m TIMESTAMPTZ,
    mfe_after_30m_pct NUMERIC,
    mae_after_30m_pct NUMERIC,

    minute_rows INTEGER NOT NULL DEFAULT 0,
    data_status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (study_start, study_end, trading_date, spot_instrument_key)
);

CREATE INDEX IF NOT EXISTS idx_spot_supply_acceptance_state15
ON public.spot_supply_acceptance_backtest
    (state_15m, outcome_after_15m, trading_date);

CREATE INDEX IF NOT EXISTS idx_spot_supply_acceptance_symbol
ON public.spot_supply_acceptance_backtest
    (symbol, trading_date);

CREATE TABLE IF NOT EXISTS public.spot_supply_acceptance_backtest_summary (
    study_start DATE NOT NULL,
    study_end DATE NOT NULL,
    run_id UUID NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expected_events INTEGER NOT NULL,
    processed_events INTEGER NOT NULL,
    valid_events INTEGER NOT NULL,
    failed_events INTEGER NOT NULL,
    summary JSONB NOT NULL,
    PRIMARY KEY (study_start, study_end)
);
"""

UPSERT = """
INSERT INTO public.spot_supply_acceptance_backtest (
    study_start,study_end,run_id,source_run_id,trading_date,symbol,spot_instrument_key,
    breakout_time,breakout_entry_price,strong_supply_low,strong_supply_high,breakout_pct,
    original_t1_s1_outcome,original_mfe_pct,original_mae_pct,

    state_5m,close_5m,close_dist_supply_5m_pct,min_dist_supply_5m_pct,retested_5m,
    entry_after_5m,outcome_after_5m,outcome_time_after_5m,mfe_after_5m_pct,mae_after_5m_pct,

    state_15m,close_15m,close_dist_supply_15m_pct,min_dist_supply_15m_pct,retested_15m,
    entry_after_15m,outcome_after_15m,outcome_time_after_15m,mfe_after_15m_pct,mae_after_15m_pct,

    state_30m,close_30m,close_dist_supply_30m_pct,min_dist_supply_30m_pct,retested_30m,
    entry_after_30m,outcome_after_30m,outcome_time_after_30m,mfe_after_30m_pct,mae_after_30m_pct,

    minute_rows,data_status,error_message
) VALUES (
    %(study_start)s,%(study_end)s,%(run_id)s,%(source_run_id)s,%(trading_date)s,%(symbol)s,%(spot_instrument_key)s,
    %(breakout_time)s,%(breakout_entry_price)s,%(strong_supply_low)s,%(strong_supply_high)s,%(breakout_pct)s,
    %(original_t1_s1_outcome)s,%(original_mfe_pct)s,%(original_mae_pct)s,

    %(state_5m)s,%(close_5m)s,%(close_dist_supply_5m_pct)s,%(min_dist_supply_5m_pct)s,%(retested_5m)s,
    %(entry_after_5m)s,%(outcome_after_5m)s,%(outcome_time_after_5m)s,%(mfe_after_5m_pct)s,%(mae_after_5m_pct)s,

    %(state_15m)s,%(close_15m)s,%(close_dist_supply_15m_pct)s,%(min_dist_supply_15m_pct)s,%(retested_15m)s,
    %(entry_after_15m)s,%(outcome_after_15m)s,%(outcome_time_after_15m)s,%(mfe_after_15m_pct)s,%(mae_after_15m_pct)s,

    %(state_30m)s,%(close_30m)s,%(close_dist_supply_30m_pct)s,%(min_dist_supply_30m_pct)s,%(retested_30m)s,
    %(entry_after_30m)s,%(outcome_after_30m)s,%(outcome_time_after_30m)s,%(mfe_after_30m_pct)s,%(mae_after_30m_pct)s,

    %(minute_rows)s,%(data_status)s,%(error_message)s
)
ON CONFLICT (study_start,study_end,trading_date,spot_instrument_key) DO UPDATE SET
    run_id=EXCLUDED.run_id,
    source_run_id=EXCLUDED.source_run_id,
    symbol=EXCLUDED.symbol,
    breakout_time=EXCLUDED.breakout_time,
    breakout_entry_price=EXCLUDED.breakout_entry_price,
    strong_supply_low=EXCLUDED.strong_supply_low,
    strong_supply_high=EXCLUDED.strong_supply_high,
    breakout_pct=EXCLUDED.breakout_pct,
    original_t1_s1_outcome=EXCLUDED.original_t1_s1_outcome,
    original_mfe_pct=EXCLUDED.original_mfe_pct,
    original_mae_pct=EXCLUDED.original_mae_pct,

    state_5m=EXCLUDED.state_5m,
    close_5m=EXCLUDED.close_5m,
    close_dist_supply_5m_pct=EXCLUDED.close_dist_supply_5m_pct,
    min_dist_supply_5m_pct=EXCLUDED.min_dist_supply_5m_pct,
    retested_5m=EXCLUDED.retested_5m,
    entry_after_5m=EXCLUDED.entry_after_5m,
    outcome_after_5m=EXCLUDED.outcome_after_5m,
    outcome_time_after_5m=EXCLUDED.outcome_time_after_5m,
    mfe_after_5m_pct=EXCLUDED.mfe_after_5m_pct,
    mae_after_5m_pct=EXCLUDED.mae_after_5m_pct,

    state_15m=EXCLUDED.state_15m,
    close_15m=EXCLUDED.close_15m,
    close_dist_supply_15m_pct=EXCLUDED.close_dist_supply_15m_pct,
    min_dist_supply_15m_pct=EXCLUDED.min_dist_supply_15m_pct,
    retested_15m=EXCLUDED.retested_15m,
    entry_after_15m=EXCLUDED.entry_after_15m,
    outcome_after_15m=EXCLUDED.outcome_after_15m,
    outcome_time_after_15m=EXCLUDED.outcome_time_after_15m,
    mfe_after_15m_pct=EXCLUDED.mfe_after_15m_pct,
    mae_after_15m_pct=EXCLUDED.mae_after_15m_pct,

    state_30m=EXCLUDED.state_30m,
    close_30m=EXCLUDED.close_30m,
    close_dist_supply_30m_pct=EXCLUDED.close_dist_supply_30m_pct,
    min_dist_supply_30m_pct=EXCLUDED.min_dist_supply_30m_pct,
    retested_30m=EXCLUDED.retested_30m,
    entry_after_30m=EXCLUDED.entry_after_30m,
    outcome_after_30m=EXCLUDED.outcome_after_30m,
    outcome_time_after_30m=EXCLUDED.outcome_time_after_30m,
    mfe_after_30m_pct=EXCLUDED.mfe_after_30m_pct,
    mae_after_30m_pct=EXCLUDED.mae_after_30m_pct,

    minute_rows=EXCLUDED.minute_rows,
    data_status=EXCLUDED.data_status,
    error_message=EXCLUDED.error_message,
    updated_at=NOW();
"""


def ensure_schema() -> None:
    with psycopg.connect(DATABASE_URL, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()


def pct(value: float, base: float) -> float | None:
    if not base:
        return None
    return (value / base - 1.0) * 100.0


def classify_window(rows: list[dict[str, Any]], supply: float) -> dict[str, Any]:
    if not rows:
        raise ValueError("empty confirmation window")
    min_low = min(r["low"] for r in rows)
    last_close = rows[-1]["close"]
    min_dist = pct(min_low, supply)
    close_dist = pct(last_close, supply)
    failed_level = supply * (1.0 - FAIL_THRESHOLD_PCT / 100.0)

    if min_low <= failed_level:
        state = "FAILED_BELOW"
    elif min_low > supply:
        state = "HELD_ABOVE"
    elif last_close > supply:
        state = "RETESTED_AND_RECOVERED"
    else:
        state = "CLOSED_BELOW"

    return {
        "state": state,
        "close": last_close,
        "close_dist": close_dist,
        "min_dist": min_dist,
        "retested": min_low <= supply,
    }


def forward_outcome(
    rows: list[dict[str, Any]],
    start_ts: datetime,
    entry: float,
) -> dict[str, Any]:
    target = entry * (1.0 + TARGET_PCT / 100.0)
    stop = entry * (1.0 - STOP_PCT / 100.0)

    future = [
        r for r in rows
        if r["ts"] >= start_ts and r["ts"].time().replace(tzinfo=None) <= MARKET_CLOSE
    ]
    if not future:
        return {"outcome": "NEITHER", "ts": None, "mfe": None, "mae": None}

    mfe = (max(r["high"] for r in future) / entry - 1.0) * 100.0
    mae = (min(r["low"] for r in future) / entry - 1.0) * 100.0

    for r in future:
        hit_t = r["high"] >= target
        hit_s = r["low"] <= stop
        if hit_t and hit_s:
            return {"outcome": "AMBIGUOUS_SAME_BAR", "ts": r["ts"], "mfe": mfe, "mae": mae}
        if hit_t:
            return {"outcome": "TARGET_FIRST", "ts": r["ts"], "mfe": mfe, "mae": mae}
        if hit_s:
            return {"outcome": "STOP_FIRST", "ts": r["ts"], "mfe": mfe, "mae": mae}
    return {"outcome": "NEITHER", "ts": None, "mfe": mfe, "mae": mae}


def blank_payload(event: dict[str, Any]) -> dict[str, Any]:
    row = dict(event)
    row.update({
        "study_start": STUDY_START,
        "study_end": STUDY_END,
        "run_id": RUN_ID,
        "minute_rows": 0,
        "data_status": "ERROR",
        "error_message": None,
    })
    for w in WINDOWS:
        row.update({
            f"state_{w}m": None,
            f"close_{w}m": None,
            f"close_dist_supply_{w}m_pct": None,
            f"min_dist_supply_{w}m_pct": None,
            f"retested_{w}m": None,
            f"entry_after_{w}m": None,
            f"outcome_after_{w}m": None,
            f"outcome_time_after_{w}m": None,
            f"mfe_after_{w}m_pct": None,
            f"mae_after_{w}m_pct": None,
        })
    return row


def analyze_event(event: dict[str, Any], all_minutes: list[dict[str, Any]]) -> dict[str, Any]:
    row = blank_payload(event)
    day = event["trading_date"]
    breakout = event["breakout_time"].astimezone(IST)
    supply = float(event["strong_supply_high"])

    mins = [
        r for r in all_minutes
        if r["ts"].date() == day
        and BREAKOUT_TIME <= r["ts"].time().replace(tzinfo=None) <= MARKET_CLOSE
    ]
    row["minute_rows"] = len(mins)
    if len(mins) < 60:
        raise ValueError(f"insufficient 1-minute rows after 10:15: {len(mins)}")

    for w in WINDOWS:
        end_ts = breakout + timedelta(minutes=w)
        window = [r for r in mins if breakout <= r["ts"] < end_ts]
        # 5m/15m/30m windows should be close to complete. Tolerate one missing minute.
        if len(window) < max(1, w - 1):
            raise ValueError(f"{w}m window incomplete: {len(window)}/{w}")

        cls = classify_window(window, supply)
        entry = float(cls["close"])
        fwd = forward_outcome(mins, end_ts, entry)

        row[f"state_{w}m"] = cls["state"]
        row[f"close_{w}m"] = cls["close"]
        row[f"close_dist_supply_{w}m_pct"] = cls["close_dist"]
        row[f"min_dist_supply_{w}m_pct"] = cls["min_dist"]
        row[f"retested_{w}m"] = cls["retested"]
        row[f"entry_after_{w}m"] = entry
        row[f"outcome_after_{w}m"] = fwd["outcome"]
        row[f"outcome_time_after_{w}m"] = fwd["ts"]
        row[f"mfe_after_{w}m_pct"] = fwd["mfe"]
        row[f"mae_after_{w}m_pct"] = fwd["mae"]

    row["data_status"] = "OK"
    row["error_message"] = None
    return row


def write_rows(rows: list[dict[str, Any]]) -> None:
    with psycopg.connect(DATABASE_URL, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT, rows)
        conn.commit()


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "run_id": RUN_ID,
        "study_type": "10:15_SPOT_SUPPLY_BREAKOUT_ACCEPTANCE_5_15_30M",
        "study_start": STUDY_START.isoformat(),
        "study_end": STUDY_END.isoformat(),
        "expected_events": EXPECTED_EVENTS,
        "processed_events": len(rows),
        "valid_events": sum(r["data_status"] == "OK" for r in rows),
        "failed_events": sum(r["data_status"] != "OK" for r in rows),
        "failure_threshold_pct_below_supply": FAIL_THRESHOLD_PCT,
        "forward_target_pct": TARGET_PCT,
        "forward_stop_pct": STOP_PCT,
    }

    valid = [r for r in rows if r["data_status"] == "OK"]
    for w in WINDOWS:
        by_state: dict[str, dict[str, int | float | None]] = {}
        states = sorted({r[f"state_{w}m"] for r in valid if r[f"state_{w}m"]})
        for state in states:
            group = [r for r in valid if r[f"state_{w}m"] == state]
            outcomes = [r[f"outcome_after_{w}m"] for r in group]
            resolved = sum(o in {"TARGET_FIRST", "STOP_FIRST"} for o in outcomes)
            wins = sum(o == "TARGET_FIRST" for o in outcomes)
            stops = sum(o == "STOP_FIRST" for o in outcomes)
            neither = sum(o == "NEITHER" for o in outcomes)
            amb = sum(o == "AMBIGUOUS_SAME_BAR" for o in outcomes)
            by_state[state] = {
                "events": len(group),
                "resolved": resolved,
                "target_first": wins,
                "stop_first": stops,
                "neither": neither,
                "ambiguous": amb,
                "target_first_pct_resolved": round(wins / resolved * 100.0, 2) if resolved else None,
            }
        summary[f"window_{w}m"] = by_state
    return summary


def write_summary(summary: dict[str, Any]) -> None:
    sql = """
    INSERT INTO public.spot_supply_acceptance_backtest_summary (
        study_start,study_end,run_id,expected_events,processed_events,valid_events,failed_events,summary
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (study_start,study_end) DO UPDATE SET
        run_id=EXCLUDED.run_id,
        generated_at=NOW(),
        expected_events=EXCLUDED.expected_events,
        processed_events=EXCLUDED.processed_events,
        valid_events=EXCLUDED.valid_events,
        failed_events=EXCLUDED.failed_events,
        summary=EXCLUDED.summary;
    """
    with psycopg.connect(DATABASE_URL, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                STUDY_START, STUDY_END, RUN_ID,
                summary["expected_events"], summary["processed_events"],
                summary["valid_events"], summary["failed_events"],
                Jsonb(summary),
            ))
        conn.commit()


def main() -> None:
    if not TOKEN:
        raise RuntimeError("UPSTOX_TOKEN or UPSTOX_ACCESS_TOKEN is required")
    if not DATABASE_URL:
        raise RuntimeError("NEON_DATABASE_URL is required")

    ensure_schema()
    events = load_1015_events()

    log("=" * 90)
    log("10:15 STRONG SUPPLY ACCEPTANCE BACKTEST")
    log(f"run_id={RUN_ID}")
    log(f"Neon 10:15 events loaded={len(events)} | expected={EXPECTED_EVENTS}")
    log("=" * 90)

    # Important safety check: this worker is intentionally scoped to the known 88 events only.
    if len(events) != EXPECTED_EVENTS:
        raise RuntimeError(
            f"Expected exactly {EXPECTED_EVENTS} 10:15 events, but Neon returned {len(events)}. "
            "Refusing to run a different sample."
        )

    # Group by instrument so one historical API request can cover all dates for that stock.
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[event["spot_instrument_key"]].append(event)

    analyzed: list[dict[str, Any]] = []
    for idx, (key, group) in enumerate(grouped.items(), 1):
        symbol = group[0]["symbol"]
        min_day = min(e["trading_date"] for e in group)
        max_day = max(e["trading_date"] for e in group)
        try:
            candles = parse_candles(historical_1m(key, min_day, max_day))
            if not candles:
                raise ValueError("no 1-minute history returned")
            for event in group:
                try:
                    analyzed.append(analyze_event(event, candles))
                except Exception as exc:
                    bad = blank_payload(event)
                    bad["error_message"] = str(exc)[:1000]
                    analyzed.append(bad)
        except Exception as exc:
            for event in group:
                bad = blank_payload(event)
                bad["error_message"] = str(exc)[:1000]
                analyzed.append(bad)

        if idx % 10 == 0 or idx == len(grouped):
            log(f"Historical fetch progress: {idx}/{len(grouped)} instruments | rows prepared={len(analyzed)}")
        time.sleep(0.12)

    if len(analyzed) != EXPECTED_EVENTS:
        raise RuntimeError(f"Internal row-count error: prepared {len(analyzed)} rows, expected {EXPECTED_EVENTS}")

    write_rows(analyzed)
    summary = build_summary(analyzed)
    write_summary(summary)

    log("=" * 90)
    log("ACCEPTANCE BACKTEST COMPLETE")
    log(f"processed={summary['processed_events']} valid={summary['valid_events']} failed={summary['failed_events']}")
    for w in WINDOWS:
        log(f"{w}M RESULTS: {json.dumps(summary[f'window_{w}m'], sort_keys=True)}")
    log("Neon detail table : public.spot_supply_acceptance_backtest")
    log("Neon summary table: public.spot_supply_acceptance_backtest_summary")
    log("=" * 90)


if __name__ == "__main__":
    main()
