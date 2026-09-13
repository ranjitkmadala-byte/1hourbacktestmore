
import os, time, uuid
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

MARKET_OPEN = dtime(9, 15)
ENTRY_START = dtime(10, 15)
MARKET_CLOSE = dtime(15, 30)

STOP_BUFFER_PCT = float(os.getenv("AVWAP_STOP_BUFFER_PCT", "0.25"))
RUN_ID = str(uuid.uuid4())
API = "https://api.upstox.com/v3/historical-candle"


def log(msg):
    print(f"{datetime.now(IST):%Y-%m-%d %H:%M:%S} IST | {msg}", flush=True)


def pct(x, base):
    return (x / base - 1.0) * 100.0 if base else None


def fetch_1m(key, d1, d2):
    url = f"{API}/{quote(key, safe='')}/minutes/1/{d2.isoformat()}/{d1.isoformat()}"
    last = None
    for n in range(5):
        try:
            r = requests.get(
                url,
                headers={"Accept": "application/json", "Authorization": f"Bearer {TOKEN}"},
                timeout=60,
            )
            if r.status_code == 429:
                time.sleep(2 * (n + 1))
                continue
            r.raise_for_status()
            return r.json().get("data", {}).get("candles", []) or []
        except Exception as exc:
            last = exc
            if n < 4:
                time.sleep(1.5 * (n + 1))
    raise RuntimeError(f"Upstox history fetch failed: {last}")


def parse_1m(rows):
    out = []
    for c in rows:
        if len(c) < 6:
            continue
        try:
            ts = datetime.fromisoformat(str(c[0]).replace("Z", "+00:00")).astimezone(IST)
            out.append(
                {
                    "ts": ts,
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": int(float(c[5] or 0)),
                }
            )
        except Exception:
            continue
    return sorted(out, key=lambda x: x["ts"])


def build_3m(rows):
    buckets = {}
    for r in rows:
        tt = r["ts"].time().replace(tzinfo=None)
        if not (MARKET_OPEN <= tt < MARKET_CLOSE):
            continue
        open_dt = datetime.combine(r["ts"].date(), MARKET_OPEN, tzinfo=IST)
        mins = int((r["ts"] - open_dt).total_seconds() // 60)
        start = open_dt + timedelta(minutes=(mins // 3) * 3)
        if start not in buckets:
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
            b = buckets[start]
            b["high"] = max(b["high"], r["high"])
            b["low"] = min(b["low"], r["low"])
            b["close"] = r["close"]
            b["volume"] += r["volume"]

    bars = [buckets[k] for k in sorted(buckets)]

    cum_pv = 0.0
    cum_v = 0.0
    for b in bars:
        typical = (b["high"] + b["low"] + b["close"]) / 3.0
        vol = max(0, int(b["volume"] or 0))
        cum_pv += typical * vol
        cum_v += vol
        b["avwap"] = cum_pv / cum_v if cum_v > 0 else typical
    return bars


def load_events():
    q = """
    SELECT
        run_id AS source_run_id,
        trading_date,
        symbol,
        spot_instrument_key,
        breakout_time,
        entry_price AS breakout_1015_price,
        strong_supply_high,
        breakout_pct
    FROM public.spot_supply_1h_backtest_events
    WHERE study_start=%s
      AND study_end=%s
      AND (breakout_time AT TIME ZONE 'Asia/Kolkata')::time = TIME '10:15'
    ORDER BY trading_date, symbol
    """
    with psycopg.connect(DB, row_factory=dict_row, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (START, END))
            return [dict(r) for r in cur.fetchall()]


DDL = """
CREATE TABLE IF NOT EXISTS public.spot_supply_1015_avwap_retrace_entry_backtest (
    study_start DATE NOT NULL,
    study_end DATE NOT NULL,
    run_id UUID NOT NULL,
    source_run_id UUID,
    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    spot_instrument_key TEXT NOT NULL,

    breakout_time TIMESTAMPTZ NOT NULL,
    breakout_1015_price NUMERIC NOT NULL,
    strong_supply_high NUMERIC,
    breakout_pct NUMERIC,

    anchor_bar_start TIMESTAMPTZ,
    anchor_3m_low NUMERIC,
    avwap_at_1015 NUMERIC,

    retrace_found BOOLEAN,
    retrace_bar_start TIMESTAMPTZ,
    retrace_bar_end TIMESTAMPTZ,
    retrace_low NUMERIC,
    avwap_at_retrace NUMERIC,
    retrace_depth_pct NUMERIC,
    minutes_to_retrace INTEGER,

    recovery_entry_found BOOLEAN,
    entry_bar_start TIMESTAMPTZ,
    entry_time TIMESTAMPTZ,
    entry_price NUMERIC,
    avwap_at_entry NUMERIC,
    entry_distance_above_avwap_pct NUMERIC,
    minutes_retrace_to_entry INTEGER,
    minutes_1015_to_entry INTEGER,
    entry_vs_1015_pct NUMERIC,

    final_price_1530 NUMERIC,
    eod_return_from_entry_pct NUMERIC,
    max_favorable_from_entry_pct NUMERIC,
    max_adverse_from_entry_pct NUMERIC,
    hit_0_5 BOOLEAN,
    hit_1_0 BOOLEAN,
    hit_1_5 BOOLEAN,
    hit_2_0 BOOLEAN,

    stop_0_25_below_avwap_hit BOOLEAN,
    stop_time TIMESTAMPTZ,
    stop_price NUMERIC,
    return_at_stop_pct NUMERIC,
    strategy_exit_return_pct NUMERIC,

    setup_status TEXT NOT NULL,
    data_status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (study_start, study_end, trading_date, spot_instrument_key)
);

CREATE TABLE IF NOT EXISTS public.spot_supply_1015_avwap_retrace_entry_summary (
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
INSERT INTO public.spot_supply_1015_avwap_retrace_entry_backtest (
 study_start,study_end,run_id,source_run_id,trading_date,symbol,spot_instrument_key,
 breakout_time,breakout_1015_price,strong_supply_high,breakout_pct,
 anchor_bar_start,anchor_3m_low,avwap_at_1015,
 retrace_found,retrace_bar_start,retrace_bar_end,retrace_low,avwap_at_retrace,retrace_depth_pct,minutes_to_retrace,
 recovery_entry_found,entry_bar_start,entry_time,entry_price,avwap_at_entry,entry_distance_above_avwap_pct,
 minutes_retrace_to_entry,minutes_1015_to_entry,entry_vs_1015_pct,
 final_price_1530,eod_return_from_entry_pct,max_favorable_from_entry_pct,max_adverse_from_entry_pct,
 hit_0_5,hit_1_0,hit_1_5,hit_2_0,
 stop_0_25_below_avwap_hit,stop_time,stop_price,return_at_stop_pct,strategy_exit_return_pct,
 setup_status,data_status,error_message
) VALUES (
 %(study_start)s,%(study_end)s,%(run_id)s,%(source_run_id)s,%(trading_date)s,%(symbol)s,%(spot_instrument_key)s,
 %(breakout_time)s,%(breakout_1015_price)s,%(strong_supply_high)s,%(breakout_pct)s,
 %(anchor_bar_start)s,%(anchor_3m_low)s,%(avwap_at_1015)s,
 %(retrace_found)s,%(retrace_bar_start)s,%(retrace_bar_end)s,%(retrace_low)s,%(avwap_at_retrace)s,%(retrace_depth_pct)s,%(minutes_to_retrace)s,
 %(recovery_entry_found)s,%(entry_bar_start)s,%(entry_time)s,%(entry_price)s,%(avwap_at_entry)s,%(entry_distance_above_avwap_pct)s,
 %(minutes_retrace_to_entry)s,%(minutes_1015_to_entry)s,%(entry_vs_1015_pct)s,
 %(final_price_1530)s,%(eod_return_from_entry_pct)s,%(max_favorable_from_entry_pct)s,%(max_adverse_from_entry_pct)s,
 %(hit_0_5)s,%(hit_1_0)s,%(hit_1_5)s,%(hit_2_0)s,
 %(stop_0_25_below_avwap_hit)s,%(stop_time)s,%(stop_price)s,%(return_at_stop_pct)s,%(strategy_exit_return_pct)s,
 %(setup_status)s,%(data_status)s,%(error_message)s
)
ON CONFLICT(study_start,study_end,trading_date,spot_instrument_key) DO UPDATE SET
 run_id=EXCLUDED.run_id,
 source_run_id=EXCLUDED.source_run_id,
 symbol=EXCLUDED.symbol,
 breakout_time=EXCLUDED.breakout_time,
 breakout_1015_price=EXCLUDED.breakout_1015_price,
 strong_supply_high=EXCLUDED.strong_supply_high,
 breakout_pct=EXCLUDED.breakout_pct,
 anchor_bar_start=EXCLUDED.anchor_bar_start,
 anchor_3m_low=EXCLUDED.anchor_3m_low,
 avwap_at_1015=EXCLUDED.avwap_at_1015,
 retrace_found=EXCLUDED.retrace_found,
 retrace_bar_start=EXCLUDED.retrace_bar_start,
 retrace_bar_end=EXCLUDED.retrace_bar_end,
 retrace_low=EXCLUDED.retrace_low,
 avwap_at_retrace=EXCLUDED.avwap_at_retrace,
 retrace_depth_pct=EXCLUDED.retrace_depth_pct,
 minutes_to_retrace=EXCLUDED.minutes_to_retrace,
 recovery_entry_found=EXCLUDED.recovery_entry_found,
 entry_bar_start=EXCLUDED.entry_bar_start,
 entry_time=EXCLUDED.entry_time,
 entry_price=EXCLUDED.entry_price,
 avwap_at_entry=EXCLUDED.avwap_at_entry,
 entry_distance_above_avwap_pct=EXCLUDED.entry_distance_above_avwap_pct,
 minutes_retrace_to_entry=EXCLUDED.minutes_retrace_to_entry,
 minutes_1015_to_entry=EXCLUDED.minutes_1015_to_entry,
 entry_vs_1015_pct=EXCLUDED.entry_vs_1015_pct,
 final_price_1530=EXCLUDED.final_price_1530,
 eod_return_from_entry_pct=EXCLUDED.eod_return_from_entry_pct,
 max_favorable_from_entry_pct=EXCLUDED.max_favorable_from_entry_pct,
 max_adverse_from_entry_pct=EXCLUDED.max_adverse_from_entry_pct,
 hit_0_5=EXCLUDED.hit_0_5,
 hit_1_0=EXCLUDED.hit_1_0,
 hit_1_5=EXCLUDED.hit_1_5,
 hit_2_0=EXCLUDED.hit_2_0,
 stop_0_25_below_avwap_hit=EXCLUDED.stop_0_25_below_avwap_hit,
 stop_time=EXCLUDED.stop_time,
 stop_price=EXCLUDED.stop_price,
 return_at_stop_pct=EXCLUDED.return_at_stop_pct,
 strategy_exit_return_pct=EXCLUDED.strategy_exit_return_pct,
 setup_status=EXCLUDED.setup_status,
 data_status=EXCLUDED.data_status,
 error_message=EXCLUDED.error_message,
 updated_at=NOW();
"""


def analyze(e, bars):
    day = e["trading_date"]
    anchor = next((b for b in bars if b["start"].time().replace(tzinfo=None) == MARKET_OPEN), None)
    if anchor is None:
        raise ValueError("09:15 3-minute anchor bar missing")

    start_dt = datetime.combine(day, ENTRY_START, tzinfo=IST)
    post = [b for b in bars if b["start"] >= start_dt]
    if not post:
        raise ValueError("no 3-minute bars after 10:15")

    avwap_at_1015 = post[0]["avwap"]
    breakout_px = float(e["breakout_1015_price"])

    # Step 1: after breakout, wait until price actually retraces to/touches AVWAP.
    retrace = next((b for b in post if b["low"] <= b["avwap"]), None)

    row = {
        "study_start": START,
        "study_end": END,
        "run_id": RUN_ID,
        "source_run_id": e["source_run_id"],
        "trading_date": day,
        "symbol": e["symbol"],
        "spot_instrument_key": e["spot_instrument_key"],
        "breakout_time": e["breakout_time"],
        "breakout_1015_price": breakout_px,
        "strong_supply_high": e["strong_supply_high"],
        "breakout_pct": e["breakout_pct"],
        "anchor_bar_start": anchor["start"],
        "anchor_3m_low": anchor["low"],
        "avwap_at_1015": avwap_at_1015,
        "data_status": "OK",
        "error_message": None,
    }

    final_price = float(bars[-1]["close"])

    if retrace is None:
        row.update(
            retrace_found=False,
            retrace_bar_start=None, retrace_bar_end=None, retrace_low=None,
            avwap_at_retrace=None, retrace_depth_pct=None, minutes_to_retrace=None,
            recovery_entry_found=False,
            entry_bar_start=None, entry_time=None, entry_price=None, avwap_at_entry=None,
            entry_distance_above_avwap_pct=None, minutes_retrace_to_entry=None, minutes_1015_to_entry=None,
            entry_vs_1015_pct=None, final_price_1530=final_price,
            eod_return_from_entry_pct=None, max_favorable_from_entry_pct=None, max_adverse_from_entry_pct=None,
            hit_0_5=None, hit_1_0=None, hit_1_5=None, hit_2_0=None,
            stop_0_25_below_avwap_hit=None, stop_time=None, stop_price=None,
            return_at_stop_pct=None, strategy_exit_return_pct=None,
            setup_status="NO_RETRACE"
        )
        return row

    retrace_depth = pct(float(retrace["low"]), float(retrace["avwap"]))
    minutes_to_retrace = int((retrace["end"] - start_dt).total_seconds() // 60)

    # Step 2: only AFTER the retrace bar, wait for first completed 3m close back above AVWAP.
    after_retrace = [b for b in bars if b["start"] >= retrace["end"]]
    entry_bar = next((b for b in after_retrace if b["close"] > b["avwap"]), None)

    if entry_bar is None:
        row.update(
            retrace_found=True,
            retrace_bar_start=retrace["start"], retrace_bar_end=retrace["end"],
            retrace_low=retrace["low"], avwap_at_retrace=retrace["avwap"],
            retrace_depth_pct=retrace_depth, minutes_to_retrace=minutes_to_retrace,
            recovery_entry_found=False,
            entry_bar_start=None, entry_time=None, entry_price=None, avwap_at_entry=None,
            entry_distance_above_avwap_pct=None, minutes_retrace_to_entry=None, minutes_1015_to_entry=None,
            entry_vs_1015_pct=None, final_price_1530=final_price,
            eod_return_from_entry_pct=None, max_favorable_from_entry_pct=None, max_adverse_from_entry_pct=None,
            hit_0_5=None, hit_1_0=None, hit_1_5=None, hit_2_0=None,
            stop_0_25_below_avwap_hit=None, stop_time=None, stop_price=None,
            return_at_stop_pct=None, strategy_exit_return_pct=None,
            setup_status="RETRACE_NO_RECOVERY"
        )
        return row

    entry_price = float(entry_bar["close"])
    after_entry = [b for b in bars if b["start"] >= entry_bar["end"]]

    high_after = max([entry_price] + [float(b["high"]) for b in after_entry])
    low_after = min([entry_price] + [float(b["low"]) for b in after_entry])

    stop_bar = next(
        (
            b for b in after_entry
            if b["close"] < b["avwap"] * (1.0 - STOP_BUFFER_PCT / 100.0)
        ),
        None,
    )

    if stop_bar:
        stop_price = float(stop_bar["close"])
        strategy_exit = pct(stop_price, entry_price)
    else:
        stop_price = None
        strategy_exit = pct(final_price, entry_price)

    row.update(
        retrace_found=True,
        retrace_bar_start=retrace["start"],
        retrace_bar_end=retrace["end"],
        retrace_low=retrace["low"],
        avwap_at_retrace=retrace["avwap"],
        retrace_depth_pct=retrace_depth,
        minutes_to_retrace=minutes_to_retrace,
        recovery_entry_found=True,
        entry_bar_start=entry_bar["start"],
        entry_time=entry_bar["end"],
        entry_price=entry_price,
        avwap_at_entry=entry_bar["avwap"],
        entry_distance_above_avwap_pct=pct(entry_price, float(entry_bar["avwap"])),
        minutes_retrace_to_entry=int((entry_bar["end"] - retrace["end"]).total_seconds() // 60),
        minutes_1015_to_entry=int((entry_bar["end"] - start_dt).total_seconds() // 60),
        entry_vs_1015_pct=pct(entry_price, breakout_px),
        final_price_1530=final_price,
        eod_return_from_entry_pct=pct(final_price, entry_price),
        max_favorable_from_entry_pct=pct(high_after, entry_price),
        max_adverse_from_entry_pct=pct(low_after, entry_price),
        hit_0_5=high_after >= entry_price * 1.005,
        hit_1_0=high_after >= entry_price * 1.010,
        hit_1_5=high_after >= entry_price * 1.015,
        hit_2_0=high_after >= entry_price * 1.020,
        stop_0_25_below_avwap_hit=stop_bar is not None,
        stop_time=stop_bar["end"] if stop_bar else None,
        stop_price=stop_price,
        return_at_stop_pct=pct(stop_price, entry_price) if stop_bar else None,
        strategy_exit_return_pct=strategy_exit,
        setup_status="ENTRY_TAKEN",
    )
    return row


def error_row(e, exc):
    row = {
        "study_start": START, "study_end": END, "run_id": RUN_ID, "source_run_id": e.get("source_run_id"),
        "trading_date": e["trading_date"], "symbol": e["symbol"], "spot_instrument_key": e["spot_instrument_key"],
        "breakout_time": e["breakout_time"], "breakout_1015_price": e["breakout_1015_price"],
        "strong_supply_high": e.get("strong_supply_high"), "breakout_pct": e.get("breakout_pct"),
        "anchor_bar_start": None, "anchor_3m_low": None, "avwap_at_1015": None,
        "data_status": "ERROR", "error_message": str(exc)[:1000], "setup_status": "ERROR"
    }
    nullable = [
        "retrace_found","retrace_bar_start","retrace_bar_end","retrace_low","avwap_at_retrace","retrace_depth_pct","minutes_to_retrace",
        "recovery_entry_found","entry_bar_start","entry_time","entry_price","avwap_at_entry","entry_distance_above_avwap_pct",
        "minutes_retrace_to_entry","minutes_1015_to_entry","entry_vs_1015_pct","final_price_1530","eod_return_from_entry_pct",
        "max_favorable_from_entry_pct","max_adverse_from_entry_pct","hit_0_5","hit_1_0","hit_1_5","hit_2_0",
        "stop_0_25_below_avwap_hit","stop_time","stop_price","return_at_stop_pct","strategy_exit_return_pct"
    ]
    for k in nullable:
        row[k] = None
    return row


def avg(vals):
    vals = [float(v) for v in vals if v is not None]
    return round(sum(vals)/len(vals), 4) if vals else None


def main():
    if not TOKEN or not DB:
        raise RuntimeError("UPSTOX_TOKEN and NEON_DATABASE_URL are required")

    with psycopg.connect(DB, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

    ev = load_events()
    log(f"Loaded {len(ev)} events | expected={EXPECTED}")
    if len(ev) != EXPECTED:
        raise RuntimeError(f"Expected exactly {EXPECTED} events, got {len(ev)}")

    grouped = defaultdict(list)
    for e in ev:
        grouped[e["spot_instrument_key"]].append(e)

    rows = []
    for idx, (key, es) in enumerate(grouped.items(), 1):
        try:
            parsed = parse_1m(fetch_1m(key, min(x["trading_date"] for x in es), max(x["trading_date"] for x in es)))
        except Exception as exc:
            parsed = []
            fetch_error = exc

        for e in es:
            try:
                day_rows = [r for r in parsed if r["ts"].date() == e["trading_date"]]
                if not day_rows:
                    raise ValueError(str(fetch_error) if "fetch_error" in locals() else "no 1-minute rows")
                rows.append(analyze(e, build_3m(day_rows)))
            except Exception as exc:
                rows.append(error_row(e, exc))

        if idx % 10 == 0 or idx == len(grouped):
            log(f"Progress {idx}/{len(grouped)} instruments | rows={len(rows)}")
        time.sleep(0.12)

    with psycopg.connect(DB, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT, rows)
        conn.commit()

    ok = [r for r in rows if r["data_status"] == "OK"]
    entered = [r for r in ok if r["setup_status"] == "ENTRY_TAKEN"]
    no_retrace = [r for r in ok if r["setup_status"] == "NO_RETRACE"]
    no_recovery = [r for r in ok if r["setup_status"] == "RETRACE_NO_RECOVERY"]

    summary = {
        "run_id": RUN_ID,
        "expected_events": EXPECTED,
        "valid_events": len(ok),
        "failed_events": len(rows) - len(ok),
        "entry_taken": len(entered),
        "entry_taken_pct": round(100.0 * len(entered)/len(ok), 2) if ok else None,
        "no_retrace": len(no_retrace),
        "retrace_no_recovery": len(no_recovery),
        "avg_minutes_to_retrace": avg([r["minutes_to_retrace"] for r in ok if r["retrace_found"]]),
        "avg_minutes_retrace_to_entry": avg([r["minutes_retrace_to_entry"] for r in entered]),
        "avg_minutes_1015_to_entry": avg([r["minutes_1015_to_entry"] for r in entered]),
        "avg_entry_vs_1015_pct": avg([r["entry_vs_1015_pct"] for r in entered]),
        "positive_eod_from_entry": sum(float(r["eod_return_from_entry_pct"]) > 0 for r in entered),
        "avg_eod_return_from_entry_pct": avg([r["eod_return_from_entry_pct"] for r in entered]),
        "avg_mfe_from_entry_pct": avg([r["max_favorable_from_entry_pct"] for r in entered]),
        "avg_mae_from_entry_pct": avg([r["max_adverse_from_entry_pct"] for r in entered]),
        "hit_0_5": sum(bool(r["hit_0_5"]) for r in entered),
        "hit_1_0": sum(bool(r["hit_1_0"]) for r in entered),
        "hit_1_5": sum(bool(r["hit_1_5"]) for r in entered),
        "hit_2_0": sum(bool(r["hit_2_0"]) for r in entered),
        "stop_0_25_below_avwap_hits": sum(bool(r["stop_0_25_below_avwap_hit"]) for r in entered),
        "avg_strategy_exit_return_pct": avg([r["strategy_exit_return_pct"] for r in entered]),
        "stop_buffer_pct": STOP_BUFFER_PCT,
    }

    q = """
    INSERT INTO public.spot_supply_1015_avwap_retrace_entry_summary
    (study_start,study_end,run_id,expected_events,valid_events,failed_events,summary)
    VALUES(%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(study_start,study_end) DO UPDATE SET
      run_id=EXCLUDED.run_id,
      generated_at=NOW(),
      expected_events=EXCLUDED.expected_events,
      valid_events=EXCLUDED.valid_events,
      failed_events=EXCLUDED.failed_events,
      summary=EXCLUDED.summary
    """
    with psycopg.connect(DB, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (START, END, RUN_ID, EXPECTED, len(ok), len(rows)-len(ok), Jsonb(summary)))
        conn.commit()

    log("="*90)
    log("AVWAP RETRACE + RECOVERY ENTRY BACKTEST COMPLETE")
    log(str(summary))
    log("Detail : public.spot_supply_1015_avwap_retrace_entry_backtest")
    log("Summary: public.spot_supply_1015_avwap_retrace_entry_summary")
    log("="*90)


if __name__ == "__main__":
    main()
