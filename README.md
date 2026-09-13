# 88-event 10:15 Strong Supply -> EOD study

Uses exactly the 88 existing 10:15 spot Strong Supply breakout events in Neon.

For each event:
- reference = 10:15 breakout close
- final price = final completed 1-minute spot candle at 15:30
- calculates EOD return %, EOD above breakout, EOD above Strong Supply,
  post-10:15 day high/low, MFE and MAE.

Writes:
- public.spot_supply_1015_eod_backtest
- public.spot_supply_1015_eod_summary

Required Railway variables:
- UPSTOX_TOKEN
- NEON_DATABASE_URL
