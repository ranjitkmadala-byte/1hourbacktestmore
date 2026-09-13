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

# Stop rule:
# CLOSE = a 3-minute candle close below AVWAP
# LOW   = any 3-minute candle low below AVWAP
STOP_MODE = os.getenv("AVWAP_STOP_MODE", "CLOSE").strip().upper()
if STOP_MODE not in {"CLOSE", "LOW"}:
    raise ValueError("AVWAP_STOP_MODE must be CLOSE or LOW")

RUN_ID = str(uuid.uuid4())
HIST = "https://api.upstox.com/v3/historical-candle"


def log(msg: str) -> None:
    print(f"{datetime.now(IST):%Y-%m-%d %H:%M:%S} IST | {msg}", flush=True)


def headers():
    return {"Accept": "application/json", "Authorization": f"Bearer {TOKEN}"}


def get_1m(key: str, d1: date, d2: date):
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
    """Build 3-minute bars aligned to 09:15, 09:18, 09:21, ..."""
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
    """
    AVWAP begins with the 09:15 3-minute candle.

    TradingView-style anchored VWAP is a time anchor, so the "anchor at the
    09:15 candle low" means the anchor bar is 09:15-09:18. We retain that
    candle's low as anchor_low, while AVWAP itself is cumulative
    volume-weighted typical price (H+L+C)/3 from the anchor bar onward.
    """
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
        strong_supply_high, breakout_pct,
        eod_return_pct
    FROM public.spot_supply_1015_eod_backtest
    WHERE study_start=%s
      AND study_end=%s
      AND data_status='OK'
    ORDER BY trading_date, symbol
    """
    with psycopg.connect(DB, row_factory=dict_row, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (START, END))
            return [dict(r) for r in cur.fetchall()]


DDL = """
CREATE TABLE IF NOT EXISTS public.spot_supply_1015_avwap_stop_backtest (
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

    anchor_bar_start TIMESTAMPTZ,
    anchor_bar_end TIMESTAMPTZ,
    anchor_3m_low NUMERIC,
    anchor_3m_high NUMERIC,
    anchor_3m_close NUMERIC,
    anchor_3m_volume BIGINT,

    avwap_at_1015 NUMERIC,
    entry_vs_avwap_pct NUMERIC,

    stop_mode TEXT NOT NULL,
    stop_hit BOOLEAN,
    stop_bar_start TIMESTAMPTZ,
    stop_bar_end TIMESTAMPTZ,
    stop_price NUMERIC,
    avwap_at_stop NUMERIC,
    return_at_stop_pct NUMERIC,
    minutes_to_stop INTEGER,

    final_price_1530 NUMERIC,
    eod_return_pct NUMERIC,
    exit_price_using_avwap NUMERIC,
    exit_return_using_avwap_pct NUMERIC,

    stopped_then_recovered_above_entry BOOLEAN,
    stopped_then_recovered_above_avwap BOOLEAN,

    three_minute_bars INTEGER NOT NULL DEFAULT 0,
    data_status TEXT NOT NULL,
    error_message TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (study_start, study_end, trading_date, spot_instrument_key)
);

CREATE INDEX IF NOT EXISTS idx_spot_supply_1015_avwap_stop_hit
ON public.spot_supply_1015_avwap_stop_backtest
(stop_hit, trading_date, symbol);

CREATE TABLE IF NOT EXISTS public.spot_supply_1015_avwap_stop_summary (
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

UPSERT = """
INSERT INTO public.spot_supply_1015_avwap_stop_backtest (
    study_start,study_end,run_id,source_run_id,
    trading_date,symbol,spot_instrument_key,
    breakout_time,breakout_price,strong_supply_high,breakout_pct,
    anchor_bar_start,anchor_bar_end,anchor_3m_low,anchor_3m_high,anchor_3m_close,anchor_3m_volume,
    avwap_at_1015,entry_vs_avwap_pct,
    stop_mode,stop_hit,stop_bar_start,stop_bar_end,stop_price,avwap_at_stop,return_at_stop_pct,minutes_to_stop,
    final_price_1530,eod_return_pct,exit_price_using_avwap,exit_return_using_avwap_pct,
    stopped_then_recovered_above_entry,stopped_then_recovered_above_avwap,
    three_minute_bars,data_status,error_message
) VALUES (
    %(study_start)s,%(study_end)s,%(run_id)s,%(source_run_id)s,
    %(trading_date)s,%(symbol)s,%(spot_instrument_key)s,
    %(breakout_time)s,%(breakout_price)s,%(strong_supply_high)s,%(breakout_pct)s,
    %(anchor_bar_start)s,%(anchor_bar_end)s,%(anchor_3m_low)s,%(anchor_3m_high)s,%(anchor_3m_close)s,%(anchor_3m_volume)s,
    %(avwap_at_1015)s,%(entry_vs_avwap_pct)s,
    %(stop_mode)s,%(stop_hit)s,%(stop_bar_start)s,%(stop_bar_end)s,%(stop_price)s,%(avwap_at_stop)s,%(return_at_stop_pct)s,%(minutes_to_stop)s,
    %(final_price_1530)s,%(eod_return_pct)s,%(exit_price_using_avwap)s,%(exit_return_using_avwap_pct)s,
    %(stopped_then_recovered_above_entry)s,%(stopped_then_recovered_above_avwap)s,
    %(three_minute_bars)s,%(data_status)s,%(error_message)s
)
ON CONFLICT (study_start,study_end,trading_date,spot_instrument_key) DO UPDATE SET
    run_id=EXCLUDED.run_id,
    source_run_id=EXCLUDED.source_run_id,
    symbol=EXCLUDED.symbol,
    breakout_time=EXCLUDED.breakout_time,
    breakout_price=EXCLUDED.breakout_price,
    strong_supply_high=EXCLUDED.strong_supply_high,
    breakout_pct=EXCLUDED.breakout_pct,
    anchor_bar_start=EXCLUDED.anchor_bar_start,
    anchor_bar_end=EXCLUDED.anchor_bar_end,
    anchor_3m_low=EXCLUDED.anchor_3m_low,
    anchor_3m_high=EXCLUDED.anchor_3m_high,
    anchor_3m_close=EXCLUDED.anchor_3m_close,
    anchor_3m_volume=EXCLUDED.anchor_3m_volume,
    avwap_at_1015=EXCLUDED.avwap_at_1015,
    entry_vs_avwap_pct=EXCLUDED.entry_vs_avwap_pct,
    stop_mode=EXCLUDED.stop_mode,
    stop_hit=EXCLUDED.stop_hit,
    stop_bar_start=EXCLUDED.stop_bar_start,
    stop_bar_end=EXCLUDED.stop_bar_end,
    stop_price=EXCLUDED.stop_price,
    avwap_at_stop=EXCLUDED.avwap_at_stop,
    return_at_stop_pct=EXCLUDED.return_at_stop_pct,
    minutes_to_stop=EXCLUDED.minutes_to_stop,
    final_price_1530=EXCLUDED.final_price_1530,
    eod_return_pct=EXCLUDED.eod_return_pct,
    exit_price_using_avwap=EXCLUDED.exit_price_using_avwap,
    exit_return_using_avwap_pct=EXCLUDED.exit_return_using_avwap_pct,
    stopped_then_recovered_above_entry=EXCLUDED.stopped_then_recovered_above_entry,
    stopped_then_recovered_above_avwap=EXCLUDED.stopped_then_recovered_above_avwap,
    three_minute_bars=EXCLUDED.three_minute_bars,
    data_status=EXCLUDED.data_status,
    error_message=EXCLUDED.error_message,
    updated_at=NOW();
"""


def pct(v, base):
    return ((v / base) - 1.0) * 100.0 if base else None


def analyze_event(event, bars):
    bp = float(event["breakout_price"])
    day = event["trading_date"]
    bars = [b for b in bars if b["start"].date() == day]
    if not bars:
        raise ValueError("no 3-minute bars for date")

    anchor_dt = datetime.combine(day, MARKET_OPEN, tzinfo=IST)
    anchor = next((b for b in bars if b["start"] == anchor_dt), None)
    if anchor is None:
        raise ValueError("09:15 anchor bar missing")

    bars = add_avwap(bars)

    # Entry is 10:15; use the first completed 3m bar whose start is >= 10:15.
    entry_dt = datetime.combine(day, ENTRY_TIME, tzinfo=IST)
    post = [b for b in bars if b["start"] >= entry_dt]
    if not post:
        raise ValueError("no post-10:15 3-minute bars")

    avwap_at_entry = post[0]["avwap"]
    stop_bar = None

    for b in post:
        trigger = (b["close"] < b["avwap"]) if STOP_MODE == "CLOSE" else (b["low"] < b["avwap"])
        if trigger:
            stop_bar = b
            break

    final_bar = bars[-1]
    final_price = float(final_bar["close"])

    if stop_bar:
        # CLOSE mode exits at the triggering 3-minute close.
        # LOW mode conservatively uses AVWAP as the stop execution price.
        stop_price = float(stop_bar["close"]) if STOP_MODE == "CLOSE" else float(stop_bar["avwap"])
        exit_price = stop_price
        exit_return = pct(exit_price, bp)
        stop_time = stop_bar["end"]
        minutes_to_stop = int((stop_time - entry_dt).total_seconds() // 60)

        after_stop = [b for b in bars if b["start"] >= stop_bar["end"]]
        recovered_entry = any(b["high"] > bp for b in after_stop)
        recovered_avwap = any(b["close"] > b["avwap"] for b in after_stop)
    else:
        stop_price = None
        exit_price = final_price
        exit_return = pct(final_price, bp)
        stop_time = None
        minutes_to_stop = None
        recovered_entry = False
        recovered_avwap = False

    return {
        "study_start": START,
        "study_end": END,
        "run_id": RUN_ID,
        "source_run_id": event["source_run_id"],
        "trading_date": day,
        "symbol": event["symbol"],
        "spot_instrument_key": event["spot_instrument_key"],
        "breakout_time": event["breakout_time"],
        "breakout_price": bp,
        "strong_supply_high": event["strong_supply_high"],
        "breakout_pct": event["breakout_pct"],

        "anchor_bar_start": anchor["start"],
        "anchor_bar_end": anchor["end"],
        "anchor_3m_low": anchor["low"],
        "anchor_3m_high": anchor["high"],
        "anchor_3m_close": anchor["close"],
        "anchor_3m_volume": anchor["volume"],

        "avwap_at_1015": avwap_at_entry,
        "entry_vs_avwap_pct": pct(bp, avwap_at_entry),

        "stop_mode": STOP_MODE,
        "stop_hit": stop_bar is not None,
        "stop_bar_start": stop_bar["start"] if stop_bar else None,
        "stop_bar_end": stop_bar["end"] if stop_bar else None,
        "stop_price": stop_price,
        "avwap_at_stop": stop_bar["avwap"] if stop_bar else None,
        "return_at_stop_pct": pct(stop_price, bp) if stop_bar else None,
        "minutes_to_stop": minutes_to_stop,

        "final_price_1530": final_price,
        "eod_return_pct": pct(final_price, bp),
        "exit_price_using_avwap": exit_price,
        "exit_return_using_avwap_pct": exit_return,

        "stopped_then_recovered_above_entry": recovered_entry,
        "stopped_then_recovered_above_avwap": recovered_avwap,

        "three_minute_bars": len(bars),
        "data_status": "OK",
        "error_message": None,
    }


def blank_error(event, msg):
    return {
        "study_start": START, "study_end": END, "run_id": RUN_ID, "source_run_id": event.get("source_run_id"),
        "trading_date": event["trading_date"], "symbol": event["symbol"], "spot_instrument_key": event["spot_instrument_key"],
        "breakout_time": event["breakout_time"], "breakout_price": event["breakout_price"],
        "strong_supply_high": event.get("strong_supply_high"), "breakout_pct": event.get("breakout_pct"),
        "anchor_bar_start": None, "anchor_bar_end": None, "anchor_3m_low": None, "anchor_3m_high": None,
        "anchor_3m_close": None, "anchor_3m_volume": None, "avwap_at_1015": None, "entry_vs_avwap_pct": None,
        "stop_mode": STOP_MODE, "stop_hit": None, "stop_bar_start": None, "stop_bar_end": None,
        "stop_price": None, "avwap_at_stop": None, "return_at_stop_pct": None, "minutes_to_stop": None,
        "final_price_1530": None, "eod_return_pct": None, "exit_price_using_avwap": None,
        "exit_return_using_avwap_pct": None, "stopped_then_recovered_above_entry": None,
        "stopped_then_recovered_above_avwap": None, "three_minute_bars": 0,
        "data_status": "ERROR", "error_message": str(msg)[:1000],
    }


def summarize(rows):
    ok = [r for r in rows if r["data_status"] == "OK"]
    hit = [r for r in ok if r["stop_hit"]]
    nohit = [r for r in ok if not r["stop_hit"]]

    def avg(vals):
        vals = [float(v) for v in vals if v is not None]
        return round(sum(vals) / len(vals), 4) if vals else None

    return {
        "run_id": RUN_ID,
        "study_type": "10:15_STRONG_SUPPLY_WITH_0915_3M_ANCHORED_AVWAP_STOP",
        "stop_mode": STOP_MODE,
        "expected_events": EXPECTED,
        "valid_events": len(ok),
        "failed_events": len(rows) - len(ok),
        "stop_hit": len(hit),
        "stop_not_hit": len(nohit),
        "stop_hit_pct": round(100.0 * len(hit) / len(ok), 2) if ok else None,
        "avg_return_at_stop_pct": avg([r["return_at_stop_pct"] for r in hit]),
        "avg_minutes_to_stop": avg([r["minutes_to_stop"] for r in hit]),
        "avg_exit_return_using_avwap_pct": avg([r["exit_return_using_avwap_pct"] for r in ok]),
        "avg_eod_return_without_avwap_pct": avg([r["eod_return_pct"] for r in ok]),
        "stopped_then_recovered_above_entry": sum(bool(r["stopped_then_recovered_above_entry"]) for r in hit),
        "stopped_then_recovered_above_avwap": sum(bool(r["stopped_then_recovered_above_avwap"]) for r in hit),
        "positive_eod_group_stop_hits": sum(bool(r["stop_hit"]) and float(r["eod_return_pct"]) > 0 for r in ok),
        "negative_or_flat_eod_group_stop_hits": sum(bool(r["stop_hit"]) and float(r["eod_return_pct"]) <= 0 for r in ok),
    }


def main():
    if not TOKEN:
        raise RuntimeError("UPSTOX_TOKEN or UPSTOX_ACCESS_TOKEN required")
    if not DB:
        raise RuntimeError("NEON_DATABASE_URL required")

    with psycopg.connect(DB, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

    ev = load_events()
    log(f"Loaded {len(ev)} events | expected={EXPECTED}")
    if len(ev) != EXPECTED:
        raise RuntimeError(f"Expected exactly {EXPECTED} events, got {len(ev)}")

    groups = defaultdict(list)
    for e in ev:
        groups[e["spot_instrument_key"]].append(e)

    rows = []
    for idx, (key, gs) in enumerate(groups.items(), 1):
        try:
            raw = get_1m(key, min(g["trading_date"] for g in gs), max(g["trading_date"] for g in gs))
            parsed = parse_1m(raw)
        except Exception as exc:
            parsed = []
            fetch_error = exc

        for e in gs:
            try:
                drows = [r for r in parsed if r["ts"].date() == e["trading_date"]]
                if not drows:
                    raise ValueError(str(fetch_error) if "fetch_error" in locals() else "no 1m rows")
                bars = to_3m(drows)
                rows.append(analyze_event(e, bars))
            except Exception as exc:
                rows.append(blank_error(e, exc))

        if idx % 10 == 0 or idx == len(groups):
            log(f"progress {idx}/{len(groups)} instruments | rows={len(rows)}")
        time.sleep(0.12)

    if len(rows) != EXPECTED:
        raise RuntimeError(f"Prepared {len(rows)} rows, expected {EXPECTED}")

    with psycopg.connect(DB, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT, rows)
        conn.commit()

    summary = summarize(rows)
    q = """
    INSERT INTO public.spot_supply_1015_avwap_stop_summary
    (study_start,study_end,run_id,expected_events,valid_events,failed_events,summary)
    VALUES(%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(study_start,study_end) DO UPDATE SET
      run_id=EXCLUDED.run_id, generated_at=NOW(), expected_events=EXCLUDED.expected_events,
      valid_events=EXCLUDED.valid_events, failed_events=EXCLUDED.failed_events, summary=EXCLUDED.summary
    """
    with psycopg.connect(DB, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (
                START, END, RUN_ID, EXPECTED,
                summary["valid_events"], summary["failed_events"],
                Jsonb(summary)
            ))
        conn.commit()

    log("=" * 90)
    log("AVWAP STOP BACKTEST COMPLETE")
    log(str(summary))
    log("Detail : public.spot_supply_1015_avwap_stop_backtest")
    log("Summary: public.spot_supply_1015_avwap_stop_summary")
    log("=" * 90)


if __name__ == "__main__":
    main()
