from __future__ import annotations

import os
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, time as dtime, timedelta
from urllib.parse import quote
from zoneinfo import ZoneInfo

import psycopg
import requests
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

IST = ZoneInfo("Asia/Kolkata")

TOKEN = (os.getenv("UPSTOX_ACCESS_TOKEN") or os.getenv("UPSTOX_TOKEN") or "").strip()
DB = (os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()

START = date.fromisoformat(os.getenv("STUDY_START", "2026-08-26"))
END = date.fromisoformat(os.getenv("STUDY_END", "2026-09-11"))
EXPECTED = int(os.getenv("EXPECTED_1015_EVENTS", "88"))

ENTRY_TIME = dtime(10, 15)
MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)
BAR_MINUTES = 3
DEPTH_PCT = float(os.getenv("AVWAP_DEPTH_PCT", "0.25"))

RUN_ID = str(uuid.uuid4())
HIST = "https://api.upstox.com/v3/historical-candle"


def log(msg):
    print(f"{datetime.now(IST):%Y-%m-%d %H:%M:%S} IST | {msg}", flush=True)


def headers():
    return {"Accept": "application/json", "Authorization": f"Bearer {TOKEN}"}


def get_1m(key, d1, d2):
    url = f"{HIST}/{quote(key, safe='')}/minutes/1/{d2.isoformat()}/{d1.isoformat()}"
    last = None
    for n in range(5):
        try:
            r = requests.get(url, headers=headers(), timeout=60)
            if r.status_code == 429:
                time.sleep(2 * (n + 1))
                continue
            r.raise_for_status()
            return r.json().get("data", {}).get("candles", []) or []
        except Exception as exc:
            last = exc
            if n < 4:
                time.sleep(1.5 * (n + 1))
    raise RuntimeError(f"historical request failed: {last}")


def parse_1m(rows):
    out = []
    for c in rows:
        if len(c) < 6:
            continue
        try:
            ts = datetime.fromisoformat(str(c[0]).replace("Z", "+00:00")).astimezone(IST)
            out.append({
                "ts": ts,
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": int(float(c[5] or 0)),
            })
        except Exception:
            continue
    return sorted(out, key=lambda r: r["ts"])


def to_3m(day_rows):
    buckets = {}
    day_rows = [r for r in day_rows if MARKET_OPEN <= r["ts"].time().replace(tzinfo=None) < MARKET_CLOSE]
    for r in day_rows:
        open_dt = datetime.combine(r["ts"].date(), MARKET_OPEN, tzinfo=IST)
        mins = int((r["ts"] - open_dt).total_seconds() // 60)
        idx = mins // BAR_MINUTES
        start = open_dt + timedelta(minutes=idx * BAR_MINUTES)
        b = buckets.get(start)
        if b is None:
            buckets[start] = {
                "start": start,
                "end": start + timedelta(minutes=3),
                "open": r["open"],
                "high": r["high"],
                "low": r["low"],
                "close": r["close"],
                "volume": r["volume"],
            }
        else:
            b["high"] = max(b["high"], r["high"])
            b["low"] = min(b["low"], r["low"])
            b["close"] = r["close"]
            b["volume"] += r["volume"]
    return [buckets[k] for k in sorted(buckets)]


def add_avwap(bars):
    cum_pv = 0.0
    cum_v = 0.0
    for b in bars:
        tp = (b["high"] + b["low"] + b["close"]) / 3.0
        vol = max(0, int(b["volume"] or 0))
        cum_pv += tp * vol
        cum_v += vol
        b["avwap"] = (cum_pv / cum_v) if cum_v > 0 else tp
    return bars


def load_events():
    sql = """
    SELECT
        study_start, study_end, run_id AS source_run_id,
        trading_date, symbol, spot_instrument_key,
        breakout_time, entry_price AS breakout_price,
        strong_supply_high, breakout_pct
    FROM public.spot_supply_1h_backtest_events
    WHERE study_start=%s
      AND study_end=%s
      AND (breakout_time AT TIME ZONE 'Asia/Kolkata')::time = TIME '10:15'
    ORDER BY trading_date, symbol
    """
    with psycopg.connect(DB, row_factory=dict_row, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (START, END))
            return [dict(r) for r in cur.fetchall()]


DDL = """
CREATE TABLE IF NOT EXISTS public.spot_supply_1015_avwap_stop_compare (
    study_start DATE NOT NULL,
    study_end DATE NOT NULL,
    run_id UUID NOT NULL,
    source_run_id UUID,
    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    spot_instrument_key TEXT NOT NULL,

    breakout_time TIMESTAMPTZ NOT NULL,
    breakout_price NUMERIC NOT NULL,
    strong_supply_high NUMERIC,
    breakout_pct NUMERIC,

    anchor_3m_low NUMERIC,
    avwap_at_1015 NUMERIC,
    eod_return_pct NUMERIC,

    rule_two_close_hit BOOLEAN,
    rule_two_close_stop_time TIMESTAMPTZ,
    rule_two_close_stop_price NUMERIC,
    rule_two_close_return_pct NUMERIC,
    rule_two_close_minutes_to_stop INTEGER,
    rule_two_close_exit_return_pct NUMERIC,

    rule_depth_hit BOOLEAN,
    rule_depth_threshold_pct NUMERIC,
    rule_depth_stop_time TIMESTAMPTZ,
    rule_depth_stop_price NUMERIC,
    rule_depth_avwap NUMERIC,
    rule_depth_return_pct NUMERIC,
    rule_depth_minutes_to_stop INTEGER,
    rule_depth_exit_return_pct NUMERIC,

    data_status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (study_start, study_end, trading_date, spot_instrument_key)
);

CREATE TABLE IF NOT EXISTS public.spot_supply_1015_avwap_stop_compare_summary (
    study_start DATE NOT NULL,
    study_end DATE NOT NULL,
    run_id UUID NOT NULL,
    generated_at TIMESTAMPTZ DEFAULT NOW(),
    expected_events INTEGER NOT NULL,
    valid_events INTEGER NOT NULL,
    failed_events INTEGER NOT NULL,
    summary JSONB NOT NULL,
    PRIMARY KEY (study_start, study_end)
);
"""

UPS = """
INSERT INTO public.spot_supply_1015_avwap_stop_compare (
 study_start,study_end,run_id,source_run_id,trading_date,symbol,spot_instrument_key,
 breakout_time,breakout_price,strong_supply_high,breakout_pct,
 anchor_3m_low,avwap_at_1015,eod_return_pct,
 rule_two_close_hit,rule_two_close_stop_time,rule_two_close_stop_price,
 rule_two_close_return_pct,rule_two_close_minutes_to_stop,rule_two_close_exit_return_pct,
 rule_depth_hit,rule_depth_threshold_pct,rule_depth_stop_time,rule_depth_stop_price,
 rule_depth_avwap,rule_depth_return_pct,rule_depth_minutes_to_stop,rule_depth_exit_return_pct,
 data_status,error_message
) VALUES (
 %(study_start)s,%(study_end)s,%(run_id)s,%(source_run_id)s,%(trading_date)s,%(symbol)s,%(spot_instrument_key)s,
 %(breakout_time)s,%(breakout_price)s,%(strong_supply_high)s,%(breakout_pct)s,
 %(anchor_3m_low)s,%(avwap_at_1015)s,%(eod_return_pct)s,
 %(rule_two_close_hit)s,%(rule_two_close_stop_time)s,%(rule_two_close_stop_price)s,
 %(rule_two_close_return_pct)s,%(rule_two_close_minutes_to_stop)s,%(rule_two_close_exit_return_pct)s,
 %(rule_depth_hit)s,%(rule_depth_threshold_pct)s,%(rule_depth_stop_time)s,%(rule_depth_stop_price)s,
 %(rule_depth_avwap)s,%(rule_depth_return_pct)s,%(rule_depth_minutes_to_stop)s,%(rule_depth_exit_return_pct)s,
 %(data_status)s,%(error_message)s
)
ON CONFLICT (study_start,study_end,trading_date,spot_instrument_key) DO UPDATE SET
 run_id=EXCLUDED.run_id,
 source_run_id=EXCLUDED.source_run_id,
 symbol=EXCLUDED.symbol,
 breakout_time=EXCLUDED.breakout_time,
 breakout_price=EXCLUDED.breakout_price,
 strong_supply_high=EXCLUDED.strong_supply_high,
 breakout_pct=EXCLUDED.breakout_pct,
 anchor_3m_low=EXCLUDED.anchor_3m_low,
 avwap_at_1015=EXCLUDED.avwap_at_1015,
 eod_return_pct=EXCLUDED.eod_return_pct,
 rule_two_close_hit=EXCLUDED.rule_two_close_hit,
 rule_two_close_stop_time=EXCLUDED.rule_two_close_stop_time,
 rule_two_close_stop_price=EXCLUDED.rule_two_close_stop_price,
 rule_two_close_return_pct=EXCLUDED.rule_two_close_return_pct,
 rule_two_close_minutes_to_stop=EXCLUDED.rule_two_close_minutes_to_stop,
 rule_two_close_exit_return_pct=EXCLUDED.rule_two_close_exit_return_pct,
 rule_depth_hit=EXCLUDED.rule_depth_hit,
 rule_depth_threshold_pct=EXCLUDED.rule_depth_threshold_pct,
 rule_depth_stop_time=EXCLUDED.rule_depth_stop_time,
 rule_depth_stop_price=EXCLUDED.rule_depth_stop_price,
 rule_depth_avwap=EXCLUDED.rule_depth_avwap,
 rule_depth_return_pct=EXCLUDED.rule_depth_return_pct,
 rule_depth_minutes_to_stop=EXCLUDED.rule_depth_minutes_to_stop,
 rule_depth_exit_return_pct=EXCLUDED.rule_depth_exit_return_pct,
 data_status=EXCLUDED.data_status,
 error_message=EXCLUDED.error_message,
 updated_at=NOW();
"""


def pct(v, base):
    return ((v / base) - 1.0) * 100.0 if base else None


def evaluate_rule_two_close(post, bp, entry_dt, final_price):
    prev_below = False
    for b in post:
        below = b["close"] < b["avwap"]
        if below and prev_below:
            stop_price = float(b["close"])
            return {
                "hit": True,
                "time": b["end"],
                "price": stop_price,
                "ret": pct(stop_price, bp),
                "mins": int((b["end"] - entry_dt).total_seconds() // 60),
                "exit_ret": pct(stop_price, bp),
            }
        prev_below = below
    return {"hit": False, "time": None, "price": None, "ret": None, "mins": None, "exit_ret": pct(final_price, bp)}


def evaluate_rule_depth(post, bp, entry_dt, final_price):
    for b in post:
        threshold = b["avwap"] * (1.0 - DEPTH_PCT / 100.0)
        if b["close"] < threshold:
            stop_price = float(b["close"])
            return {
                "hit": True,
                "time": b["end"],
                "price": stop_price,
                "avwap": b["avwap"],
                "ret": pct(stop_price, bp),
                "mins": int((b["end"] - entry_dt).total_seconds() // 60),
                "exit_ret": pct(stop_price, bp),
            }
    return {"hit": False, "time": None, "price": None, "avwap": None, "ret": None, "mins": None, "exit_ret": pct(final_price, bp)}


def analyze_event(e, bars):
    day = e["trading_date"]
    bp = float(e["breakout_price"])
    bars = [b for b in bars if b["start"].date() == day]
    if not bars:
        raise ValueError("no 3-minute bars")

    anchor_dt = datetime.combine(day, MARKET_OPEN, tzinfo=IST)
    anchor = next((b for b in bars if b["start"] == anchor_dt), None)
    if anchor is None:
        raise ValueError("09:15 anchor bar missing")

    bars = add_avwap(bars)
    entry_dt = datetime.combine(day, ENTRY_TIME, tzinfo=IST)
    post = [b for b in bars if b["start"] >= entry_dt]
    if not post:
        raise ValueError("no bars after 10:15")

    final_price = float(bars[-1]["close"])
    avwap_entry = float(post[0]["avwap"])
    r1 = evaluate_rule_two_close(post, bp, entry_dt, final_price)
    r2 = evaluate_rule_depth(post, bp, entry_dt, final_price)

    return {
        "study_start": START, "study_end": END, "run_id": RUN_ID, "source_run_id": e["source_run_id"],
        "trading_date": day, "symbol": e["symbol"], "spot_instrument_key": e["spot_instrument_key"],
        "breakout_time": e["breakout_time"], "breakout_price": bp,
        "strong_supply_high": e["strong_supply_high"], "breakout_pct": e["breakout_pct"],
        "anchor_3m_low": anchor["low"], "avwap_at_1015": avwap_entry,
        "eod_return_pct": pct(final_price, bp),

        "rule_two_close_hit": r1["hit"],
        "rule_two_close_stop_time": r1["time"],
        "rule_two_close_stop_price": r1["price"],
        "rule_two_close_return_pct": r1["ret"],
        "rule_two_close_minutes_to_stop": r1["mins"],
        "rule_two_close_exit_return_pct": r1["exit_ret"],

        "rule_depth_hit": r2["hit"],
        "rule_depth_threshold_pct": DEPTH_PCT,
        "rule_depth_stop_time": r2["time"],
        "rule_depth_stop_price": r2["price"],
        "rule_depth_avwap": r2["avwap"],
        "rule_depth_return_pct": r2["ret"],
        "rule_depth_minutes_to_stop": r2["mins"],
        "rule_depth_exit_return_pct": r2["exit_ret"],

        "data_status": "OK", "error_message": None,
    }


def errrow(e, exc):
    return {
        "study_start": START, "study_end": END, "run_id": RUN_ID, "source_run_id": e.get("source_run_id"),
        "trading_date": e["trading_date"], "symbol": e["symbol"], "spot_instrument_key": e["spot_instrument_key"],
        "breakout_time": e["breakout_time"], "breakout_price": e["breakout_price"],
        "strong_supply_high": e.get("strong_supply_high"), "breakout_pct": e.get("breakout_pct"),
        "anchor_3m_low": None, "avwap_at_1015": None, "eod_return_pct": None,
        "rule_two_close_hit": None, "rule_two_close_stop_time": None, "rule_two_close_stop_price": None,
        "rule_two_close_return_pct": None, "rule_two_close_minutes_to_stop": None, "rule_two_close_exit_return_pct": None,
        "rule_depth_hit": None, "rule_depth_threshold_pct": DEPTH_PCT, "rule_depth_stop_time": None,
        "rule_depth_stop_price": None, "rule_depth_avwap": None, "rule_depth_return_pct": None,
        "rule_depth_minutes_to_stop": None, "rule_depth_exit_return_pct": None,
        "data_status": "ERROR", "error_message": str(exc)[:1000],
    }


def avg(vals):
    vals = [float(x) for x in vals if x is not None]
    return round(sum(vals) / len(vals), 4) if vals else None


def rule_summary(ok, prefix):
    hit_key = f"{prefix}_hit"
    exit_key = f"{prefix}_exit_return_pct"
    ret_key = f"{prefix}_return_pct"
    mins_key = f"{prefix}_minutes_to_stop"

    hits = [r for r in ok if r[hit_key]]
    pos = [r for r in ok if float(r["eod_return_pct"]) > 0]
    neg = [r for r in ok if float(r["eod_return_pct"]) <= 0]

    return {
        "stop_hits": len(hits),
        "stop_hit_pct": round(100.0 * len(hits) / len(ok), 2),
        "positive_eod_group_stop_hits": sum(bool(r[hit_key]) for r in pos),
        "negative_or_flat_eod_group_stop_hits": sum(bool(r[hit_key]) for r in neg),
        "avg_return_at_stop_pct": avg([r[ret_key] for r in hits]),
        "avg_minutes_to_stop": avg([r[mins_key] for r in hits]),
        "avg_exit_return_pct": avg([r[exit_key] for r in ok]),
    }


def main():
    if not TOKEN or not DB:
        raise RuntimeError("UPSTOX_TOKEN and NEON_DATABASE_URL required")

    with psycopg.connect(DB, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

    ev = load_events()
    log(f"Loaded {len(ev)} events | expected={EXPECTED}")
    if len(ev) != EXPECTED:
        raise RuntimeError(f"Expected exactly {EXPECTED}, got {len(ev)}")

    groups = defaultdict(list)
    for e in ev:
        groups[e["spot_instrument_key"]].append(e)

    rows = []
    for idx, (key, gs) in enumerate(groups.items(), 1):
        try:
            parsed = parse_1m(get_1m(key, min(g["trading_date"] for g in gs), max(g["trading_date"] for g in gs)))
        except Exception as exc:
            parsed = []
            fetch_error = exc

        for e in gs:
            try:
                drows = [r for r in parsed if r["ts"].date() == e["trading_date"]]
                if not drows:
                    raise ValueError(str(fetch_error) if "fetch_error" in locals() else "no 1m rows")
                rows.append(analyze_event(e, to_3m(drows)))
            except Exception as exc:
                rows.append(errrow(e, exc))

        if idx % 10 == 0 or idx == len(groups):
            log(f"progress {idx}/{len(groups)} instruments | rows={len(rows)}")
        time.sleep(0.12)

    with psycopg.connect(DB, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.executemany(UPS, rows)
        conn.commit()

    ok = [r for r in rows if r["data_status"] == "OK"]
    summary = {
        "run_id": RUN_ID,
        "expected_events": EXPECTED,
        "valid_events": len(ok),
        "failed_events": len(rows) - len(ok),
        "baseline_avg_eod_return_pct": avg([r["eod_return_pct"] for r in ok]),
        "two_consecutive_closes_below_avwap": rule_summary(ok, "rule_two_close"),
        "close_0_25pct_below_avwap": rule_summary(ok, "rule_depth"),
        "depth_pct": DEPTH_PCT,
    }

    q = """
    INSERT INTO public.spot_supply_1015_avwap_stop_compare_summary
    (study_start,study_end,run_id,expected_events,valid_events,failed_events,summary)
    VALUES(%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(study_start,study_end) DO UPDATE SET
      run_id=EXCLUDED.run_id,generated_at=NOW(),expected_events=EXCLUDED.expected_events,
      valid_events=EXCLUDED.valid_events,failed_events=EXCLUDED.failed_events,summary=EXCLUDED.summary
    """
    with psycopg.connect(DB, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (START, END, RUN_ID, EXPECTED, len(ok), len(rows)-len(ok), Jsonb(summary)))
        conn.commit()

    log("="*90)
    log("AVWAP STOP COMPARISON COMPLETE")
    log(str(summary))
    log("Detail : public.spot_supply_1015_avwap_stop_compare")
    log("Summary: public.spot_supply_1015_avwap_stop_compare_summary")
    log("="*90)


if __name__ == "__main__":
    main()
