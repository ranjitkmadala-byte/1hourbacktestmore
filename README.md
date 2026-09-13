# 88-event AVWAP stop-loss backtest

Scope: the same 88 existing 10:15 Strong Supply breakout events.

## AVWAP definition

- Anchor bar: first 3-minute spot candle of the day, **09:15-09:18**
- The anchor candle's low is stored as `anchor_3m_low`
- AVWAP is calculated from that 09:15 anchor bar forward using cumulative volume-weighted typical price `(H+L+C)/3`

This matches the normal behavior of an anchored VWAP: the anchor is a time/bar. The chosen bar is the 09:15 candle whose low is the user's anchor reference.

## Entry

10:15 Strong Supply breakout close.

## Stop

Default:
`AVWAP_STOP_MODE=CLOSE`

A stop is triggered on the first 3-minute candle after 10:15 whose **close is below AVWAP**.

Optional:
`AVWAP_STOP_MODE=LOW`

This triggers if the 3-minute low falls below AVWAP.

## Neon output

- `public.spot_supply_1015_avwap_stop_backtest`
- `public.spot_supply_1015_avwap_stop_summary`

## Required Railway variables

- `UPSTOX_TOKEN`
- `NEON_DATABASE_URL`

Optional:
- `AVWAP_STOP_MODE=CLOSE`
- `EXPECTED_1015_EVENTS=88`
- `STUDY_START=2026-08-26`
- `STUDY_END=2026-09-11`

## Key query

```sql
SELECT
  COUNT(*) AS events,
  COUNT(*) FILTER (WHERE stop_hit) AS stop_hits,
  ROUND(100.0 * COUNT(*) FILTER (WHERE stop_hit) / COUNT(*), 2) AS stop_hit_pct,
  ROUND(AVG(return_at_stop_pct) FILTER (WHERE stop_hit), 3) AS avg_stop_return_pct,
  ROUND(AVG(exit_return_using_avwap_pct), 3) AS avg_avwap_exit_return_pct
FROM public.spot_supply_1015_avwap_stop_backtest
WHERE data_status='OK';
```
