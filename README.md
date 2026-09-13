# 88-event AVWAP stop comparison

Same 88 existing 10:15 Strong Supply breakouts.

AVWAP anchor:
- first 3-minute spot candle of the day (09:15-09:18)
- anchor reference low is stored
- AVWAP is cumulative volume-weighted typical price from that bar

Rules compared:
1. Two consecutive 3-minute closes below AVWAP
2. One 3-minute close at least 0.25% below AVWAP

Entry:
- 10:15 Strong Supply breakout close

Writes:
- public.spot_supply_1015_avwap_stop_compare
- public.spot_supply_1015_avwap_stop_compare_summary

Required Railway vars:
- UPSTOX_TOKEN
- NEON_DATABASE_URL

Optional:
- AVWAP_DEPTH_PCT=0.25
