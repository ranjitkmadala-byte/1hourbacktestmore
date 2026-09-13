# Spot Strong Supply Acceptance Backtest — 88 events only

This Railway worker is intentionally restricted to the **88 existing 10:15 first Strong Supply breakouts**
already stored in `public.spot_supply_1h_backtest_events` for 2026-08-26 through 2026-09-11.

It will abort if Neon returns anything other than exactly 88 events.

## What it tests

For each 10:15 breakout it downloads only that stock's historical spot 1-minute candles and evaluates:

- 10:15–10:20 (5-minute confirmation)
- 10:15–10:30 (15-minute confirmation)
- 10:15–10:45 (30-minute confirmation)

Acceptance states:

- `HELD_ABOVE`: every 1-minute low stayed above Strong Supply High.
- `RETESTED_AND_RECOVERED`: supply was touched/breached, but the confirmation window closed back above it.
- `CLOSED_BELOW`: confirmation close is at/below supply, without a material failure.
- `FAILED_BELOW`: the window traded at least 0.25% below Strong Supply High.

For each window, the target/stop test starts **after the confirmation window** using that window's close as the new entry:
- +1.0% target
- -1.0% stop
- `AMBIGUOUS_SAME_BAR` if both are touched inside one 1-minute candle.

## Neon output

Detail:
`public.spot_supply_acceptance_backtest`

Summary:
`public.spot_supply_acceptance_backtest_summary`

The detail table should contain exactly 88 rows when complete.

## Railway environment variables

Required:
- `UPSTOX_TOKEN` (or `UPSTOX_ACCESS_TOKEN`)
- `NEON_DATABASE_URL`

Optional:
- `EXPECTED_1015_EVENTS=88`
- `ACCEPTANCE_FAIL_THRESHOLD_PCT=0.25`
- `ACCEPTANCE_TARGET_PCT=1.0`
- `ACCEPTANCE_STOP_PCT=1.0`
- `STUDY_START=2026-08-26`
- `STUDY_END=2026-09-11`

## Start command

`python backtest_acceptance_88.py`

## Useful Neon queries

Count/health:

```sql
SELECT data_status, COUNT(*)
FROM public.spot_supply_acceptance_backtest
GROUP BY data_status;
```

15-minute acceptance:

```sql
SELECT
    state_15m,
    COUNT(*) AS events,
    COUNT(*) FILTER (WHERE outcome_after_15m='TARGET_FIRST') AS target_first,
    COUNT(*) FILTER (WHERE outcome_after_15m='STOP_FIRST') AS stop_first,
    COUNT(*) FILTER (WHERE outcome_after_15m='NEITHER') AS neither,
    ROUND(
      100.0 * COUNT(*) FILTER (WHERE outcome_after_15m='TARGET_FIRST')
      / NULLIF(COUNT(*) FILTER (WHERE outcome_after_15m IN ('TARGET_FIRST','STOP_FIRST')), 0),
      2
    ) AS win_pct_resolved
FROM public.spot_supply_acceptance_backtest
WHERE data_status='OK'
GROUP BY state_15m
ORDER BY events DESC;
```
