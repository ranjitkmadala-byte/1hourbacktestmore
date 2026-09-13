from __future__ import annotations
import os, time, uuid
from collections import defaultdict
from datetime import date, datetime, time as dtime
from urllib.parse import quote
from zoneinfo import ZoneInfo
import psycopg, requests
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

IST = ZoneInfo("Asia/Kolkata")
TOKEN=(os.getenv("UPSTOX_ACCESS_TOKEN") or os.getenv("UPSTOX_TOKEN") or "").strip()
DB=(os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
START=date.fromisoformat(os.getenv("STUDY_START","2026-08-26"))
END=date.fromisoformat(os.getenv("STUDY_END","2026-09-11"))
EXPECTED=int(os.getenv("EXPECTED_1015_EVENTS","88"))
RUN_ID=str(uuid.uuid4())
BASE="https://api.upstox.com/v3/historical-candle"

def log(x): print(f"{datetime.now(IST):%Y-%m-%d %H:%M:%S} IST | {x}",flush=True)
def hdr(): return {"Accept":"application/json","Authorization":f"Bearer {TOKEN}"}

def get1m(key, d1, d2):
    u=f"{BASE}/{quote(key,safe='')}/minutes/1/{d2.isoformat()}/{d1.isoformat()}"
    for n in range(5):
        r=requests.get(u,headers=hdr(),timeout=60)
        if r.status_code==429: time.sleep(2*(n+1)); continue
        r.raise_for_status()
        return r.json().get("data",{}).get("candles",[]) or []
    raise RuntimeError("historical request rate-limited")

def parse(rows):
    out=[]
    for c in rows:
        if len(c)<5: continue
        try:
            ts=datetime.fromisoformat(str(c[0]).replace("Z","+00:00")).astimezone(IST)
            out.append((ts,float(c[1]),float(c[2]),float(c[3]),float(c[4])))
        except: pass
    return sorted(out)

def events():
    q="""SELECT study_start,study_end,run_id source_run_id,trading_date,symbol,
    spot_instrument_key,breakout_time,entry_price,strong_supply_high,breakout_pct
    FROM public.spot_supply_1h_backtest_events
    WHERE study_start=%s AND study_end=%s
      AND (breakout_time AT TIME ZONE 'Asia/Kolkata')::time=TIME '10:15'
    ORDER BY trading_date,symbol"""
    with psycopg.connect(DB,row_factory=dict_row) as c:
        with c.cursor() as x:
            x.execute(q,(START,END)); return [dict(r) for r in x.fetchall()]

DDL="""
CREATE TABLE IF NOT EXISTS public.spot_supply_1015_eod_backtest(
 study_start DATE NOT NULL, study_end DATE NOT NULL, run_id UUID NOT NULL, source_run_id UUID,
 trading_date DATE NOT NULL, symbol TEXT NOT NULL, spot_instrument_key TEXT NOT NULL,
 breakout_time TIMESTAMPTZ NOT NULL, breakout_price NUMERIC NOT NULL,
 strong_supply_high NUMERIC NOT NULL, breakout_pct NUMERIC,
 final_price_1530 NUMERIC, final_price_time TIMESTAMPTZ,
 eod_return_pct NUMERIC, eod_above_breakout BOOLEAN, eod_above_strong_supply BOOLEAN,
 day_high_after_1015 NUMERIC, day_low_after_1015 NUMERIC,
 max_favorable_to_eod_pct NUMERIC, max_adverse_to_eod_pct NUMERIC,
 data_status TEXT NOT NULL, error_message TEXT,
 created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW(),
 PRIMARY KEY(study_start,study_end,trading_date,spot_instrument_key)
);
CREATE TABLE IF NOT EXISTS public.spot_supply_1015_eod_summary(
 study_start DATE NOT NULL,study_end DATE NOT NULL,run_id UUID NOT NULL,
 generated_at TIMESTAMPTZ DEFAULT NOW(),expected_events INTEGER,valid_events INTEGER,
 failed_events INTEGER,summary JSONB NOT NULL,PRIMARY KEY(study_start,study_end)
);"""

UPS="""INSERT INTO public.spot_supply_1015_eod_backtest(
study_start,study_end,run_id,source_run_id,trading_date,symbol,spot_instrument_key,
breakout_time,breakout_price,strong_supply_high,breakout_pct,final_price_1530,final_price_time,
eod_return_pct,eod_above_breakout,eod_above_strong_supply,day_high_after_1015,day_low_after_1015,
max_favorable_to_eod_pct,max_adverse_to_eod_pct,data_status,error_message)
VALUES(%(study_start)s,%(study_end)s,%(run_id)s,%(source_run_id)s,%(trading_date)s,%(symbol)s,
%(spot_instrument_key)s,%(breakout_time)s,%(breakout_price)s,%(strong_supply_high)s,%(breakout_pct)s,
%(final_price_1530)s,%(final_price_time)s,%(eod_return_pct)s,%(eod_above_breakout)s,
%(eod_above_strong_supply)s,%(day_high_after_1015)s,%(day_low_after_1015)s,
%(max_favorable_to_eod_pct)s,%(max_adverse_to_eod_pct)s,%(data_status)s,%(error_message)s)
ON CONFLICT(study_start,study_end,trading_date,spot_instrument_key) DO UPDATE SET
run_id=EXCLUDED.run_id,source_run_id=EXCLUDED.source_run_id,symbol=EXCLUDED.symbol,
breakout_time=EXCLUDED.breakout_time,breakout_price=EXCLUDED.breakout_price,
strong_supply_high=EXCLUDED.strong_supply_high,breakout_pct=EXCLUDED.breakout_pct,
final_price_1530=EXCLUDED.final_price_1530,final_price_time=EXCLUDED.final_price_time,
eod_return_pct=EXCLUDED.eod_return_pct,eod_above_breakout=EXCLUDED.eod_above_breakout,
eod_above_strong_supply=EXCLUDED.eod_above_strong_supply,day_high_after_1015=EXCLUDED.day_high_after_1015,
day_low_after_1015=EXCLUDED.day_low_after_1015,max_favorable_to_eod_pct=EXCLUDED.max_favorable_to_eod_pct,
max_adverse_to_eod_pct=EXCLUDED.max_adverse_to_eod_pct,data_status=EXCLUDED.data_status,
error_message=EXCLUDED.error_message,updated_at=NOW()"""

def main():
    if not TOKEN or not DB: raise RuntimeError("UPSTOX_TOKEN and NEON_DATABASE_URL required")
    with psycopg.connect(DB) as c:
        with c.cursor() as x: x.execute(DDL)
        c.commit()
    ev=events()
    log(f"Loaded {len(ev)} 10:15 events; expected {EXPECTED}")
    if len(ev)!=EXPECTED: raise RuntimeError(f"Expected exactly {EXPECTED}, got {len(ev)}")
    groups=defaultdict(list)
    for e in ev: groups[e["spot_instrument_key"]].append(e)
    out=[]
    for i,(key,gs) in enumerate(groups.items(),1):
        try:
            cs=parse(get1m(key,min(g["trading_date"] for g in gs),max(g["trading_date"] for g in gs)))
        except Exception as ex:
            cs=[]; fetcherr=str(ex)
        for e in gs:
            row={**e,"run_id":RUN_ID,"breakout_price":float(e["entry_price"]),
                 "final_price_1530":None,"final_price_time":None,"eod_return_pct":None,
                 "eod_above_breakout":None,"eod_above_strong_supply":None,
                 "day_high_after_1015":None,"day_low_after_1015":None,
                 "max_favorable_to_eod_pct":None,"max_adverse_to_eod_pct":None,
                 "data_status":"ERROR","error_message":None}
            d=e["trading_date"]; bp=float(e["entry_price"]); ss=float(e["strong_supply_high"])
            day=[z for z in cs if z[0].date()==d and dtime(10,15)<=z[0].time().replace(tzinfo=None)<=dtime(15,30)]
            if not day:
                row["error_message"]=fetcherr if 'fetcherr' in locals() else "no post-10:15 1m rows"
            else:
                # Upstox minute timestamps are candle starts; last available candle at/before 15:29
                # represents the final completed minute ending at 15:30.
                eligible=[z for z in day if z[0].time().replace(tzinfo=None)<dtime(15,30)]
                if not eligible:
                    row["error_message"]="no completed minute before 15:30"
                else:
                    last=max(eligible,key=lambda z:z[0])
                    fp=last[4]; hi=max(z[2] for z in eligible); lo=min(z[3] for z in eligible)
                    row.update(final_price_1530=fp,final_price_time=last[0],
                      eod_return_pct=(fp/bp-1)*100,eod_above_breakout=fp>bp,
                      eod_above_strong_supply=fp>ss,day_high_after_1015=hi,day_low_after_1015=lo,
                      max_favorable_to_eod_pct=(hi/bp-1)*100,max_adverse_to_eod_pct=(lo/bp-1)*100,
                      data_status="OK",error_message=None)
            out.append(row)
        if i%10==0 or i==len(groups): log(f"Progress {i}/{len(groups)} instruments, {len(out)} rows")
        time.sleep(.12)
    with psycopg.connect(DB) as c:
        with c.cursor() as x: x.executemany(UPS,out)
        c.commit()
    ok=[r for r in out if r["data_status"]=="OK"]
    vals=[r["eod_return_pct"] for r in ok]
    buckets={"GE_2":0,"1_TO_2":0,"0_5_TO_1":0,"0_TO_0_5":0,"NEG_0_TO_0_5":0,"NEG_0_5_TO_1":0,"LE_NEG_1":0}
    for v in vals:
        if v>=2:buckets["GE_2"]+=1
        elif v>=1:buckets["1_TO_2"]+=1
        elif v>=.5:buckets["0_5_TO_1"]+=1
        elif v>=0:buckets["0_TO_0_5"]+=1
        elif v>-.5:buckets["NEG_0_TO_0_5"]+=1
        elif v>-1:buckets["NEG_0_5_TO_1"]+=1
        else:buckets["LE_NEG_1"]+=1
    s={"run_id":RUN_ID,"events":len(out),"valid":len(ok),"failed":len(out)-len(ok),
       "positive_eod":sum(v>0 for v in vals),"negative_or_flat_eod":sum(v<=0 for v in vals),
       "avg_eod_return_pct":round(sum(vals)/len(vals),3) if vals else None,
       "median_eod_return_pct":sorted(vals)[len(vals)//2] if vals else None,"buckets":buckets}
    q="""INSERT INTO public.spot_supply_1015_eod_summary(study_start,study_end,run_id,expected_events,valid_events,failed_events,summary)
    VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(study_start,study_end) DO UPDATE SET run_id=EXCLUDED.run_id,
    generated_at=NOW(),expected_events=EXCLUDED.expected_events,valid_events=EXCLUDED.valid_events,
    failed_events=EXCLUDED.failed_events,summary=EXCLUDED.summary"""
    with psycopg.connect(DB) as c:
        with c.cursor() as x:x.execute(q,(START,END,RUN_ID,EXPECTED,len(ok),len(out)-len(ok),Jsonb(s)))
        c.commit()
    log(f"EOD BACKTEST COMPLETE | {s}")
    log("Neon: public.spot_supply_1015_eod_backtest / public.spot_supply_1015_eod_summary")

if __name__=="__main__": main()
