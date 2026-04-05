# DWH Pipeline Plan

## Overview


Daily batch pipeline that ingests raw loyalty/miles app events from `event_stream.csv` into BigQuery, transforming through three layers to produce growth accounting and engagement metrics.

---

## Reference Scripts (`code-projects/analytics`)

Scripts from the existing analytics pipeline that this project draws patterns from:

**Bronze (DWD) — event ingestion & cleaning**
- `etl_events_client.py`, `etl_events_server.py` — raw event ingestion
- `etl_m_events_server_login.py`, `etl_m_events_server_level.py` — typed event loaders
- `dwd_user_login.py`, `dwd_user_level_prog.py` — cleaned user event tables

**Silver (DWM) — user-level aggregations**
- `dwm_full_user_active.py` — daily activity snapshot
- `dwm_full_user_spend.py`, `dwm_full_user_purchase.py` — spend aggregations
- `dwm_full_user_segment.py` — user segmentation

**Gold (DWS) — serving metrics**
- `dws_daily_dau_sgt.py`, `dws_daily_nru_sgt.py` — DAU and new user metrics
- `dws_cohort_level_results.py`, `dws_cohort_iap_rev.py` — cohort analysis
- `dws_daily_mau_sgt.py` — MAU rollup

**Utilities**
- `utils/bq_utils.py` — BigQuery client helpers (to be adapted as `utils/bq_utils.py`)
- `utils/gcp_utils.py` — GCP auth and config helpers

---

## Folder Structure

```
dwh/
├── raw/
│   ├── event_stream.csv
│   └── event_stream.schema.md
├── bronze/
│   └── etl_event_stream.py
├── silver/
│   ├── dwm_user_daily_activity.py
│   └── dwm_user_miles_balance.py
├── gold/
│   ├── dws_growth_accounting.py
│   ├── dws_cohort_retention.py
│   └── dws_engagement_depth.py
└── plan.md
```

---

## Layers

### Raw
- Source CSV file, untouched.
- Schema documented in `raw/event_stream.schema.md`.

### Bronze — `bronze.events` (`etl_event_stream.py`)
Raw event table loaded directly from `event_stream.csv`, partitioned by `event_time` (DAY, no expiration).

**Usage:** `python bronze/etl_event_stream.py <YYYYMMDD> <config>`

Logic:
- Creates `bronze.events` table with partition expiration disabled (supports historical loads)
- Filters CSV rows to the target date only
- Idempotent: deletes existing rows for the date before re-inserting (safe to re-run)
- Loads via `load_table_from_file` using CSV batch load (no streaming cost)

Schema: `event_time` (TIMESTAMP), `user_id`, `gender`, `event_type`, `transaction_category`, `miles_amount` (FLOAT64), `platform`, `utm_source`, `country`

Planned transformations (not yet implemented):
- Split `transaction_category` into `miles_category` and `search_brand`
- Coalesce `miles_amount` to `0.0` for non-miles events
- Deduplicate on `(event_time, user_id, event_type)`
- Add `is_transactional` boolean flag

### Silver (DWM) — User Daily Activity
Aggregated per `(event_date, user_id)`.

**`dwm_user_daily_activity`**
- `is_active = TRUE`
- `total_events`, `miles_earned_sum`, `miles_redeemed_sum`
- Event type counts: `cnt_miles_earned`, `cnt_miles_redeemed`, `cnt_like`, `cnt_share`, `cnt_reward_search`

**`dwm_user_miles_balance`**
- Running net miles balance per user (`miles_earned - miles_redeemed`)

### Gold (DWS) — Serving Metrics

**`dws_growth_accounting`**  
Daily user state classification using prior period comparison:

| Metric | Definition |
|---|---|
| New | First ever active date = current date |
| Retained | Active today AND active in prior period |
| Resurrected | Active today, not in prior period, but seen before |
| Churned | Active in prior period, not active today |

**`dws_cohort_retention`**
- `cohort_date` = user's first active date
- Retention % at D+1, D+7, D+14, D+30

**`dws_engagement_depth`**
- Events per DAU per day
- Miles earned per active user
- Redemption rate: users who redeemed / users who earned
- Active users by `utm_source` (UA channel quality)

---

## Stack

- **BigQuery** — storage and transformation
- **Python** — orchestration (`google-cloud-bigquery`)
- **Great Expectations** — data quality assertions on bronze `dwd_events`
- **Cloud Scheduler / Airflow** — daily batch trigger

## Data Quality Assertions (Great Expectations)

Applied at bronze `dwd_events` layer:
- `event_time` is not null
- `user_id` matches pattern `u_\d{4}`
- `event_type` is one of: `miles_earned`, `miles_redeemed`, `reward_search`, `like`, `share`
- `miles_amount` > 0 when `event_type` in (`miles_earned`, `miles_redeemed`)
- `miles_amount` is null when `event_type` in (`like`, `share`, `reward_search`)
- `country` is one of: `MY`, `PH`, `TH`, `ID`, `SG`
