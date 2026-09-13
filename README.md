# 88-event Strong Supply -> AVWAP retrace -> 3m recovery entry backtest

Sequence tested:
1. Existing 10:15 Strong Supply breakout.
2. After 10:15, price must retrace to/touch the running AVWAP anchored from the 09:15 3-minute bar.
3. No entry on touch.
4. After the retrace bar, wait for the first completed 3-minute candle close above AVWAP.
5. Entry = that recovery candle close.
6. Measure EOD return, MFE/MAE, +0.5/+1/+1.5/+2%, and optional 0.25%-below-AVWAP stop.

NO_RETRACE and RETRACE_NO_RECOVERY are recorded separately and are not treated as entries.

Writes:
- public.spot_supply_1015_avwap_retrace_entry_backtest
- public.spot_supply_1015_avwap_retrace_entry_summary

Required Railway variables:
- UPSTOX_TOKEN
- NEON_DATABASE_URL

Optional:
- AVWAP_STOP_BUFFER_PCT=0.25
