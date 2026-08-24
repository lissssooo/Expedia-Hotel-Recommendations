# CORE schema

Build timestamp: `2026-08-13T22:11:07+00:00`
Source: immutable `raw.train`, `raw.test`, `raw.destinations`
Architecture: `RAW → STAGING → CORE`; no CLEAN/SILVER or MARTS are materialized.

## Tables

| Table | Grain | Primary key |
|---|---|---|
| `core.dim_date` | one row per valid calendar day | `date_key` |
| `core.dim_hour` | one row per hour of day | `hour_key` |
| `core.dim_user` | one row per user | `user_id` |
| `core.dim_user_location` | one row per observed user country/region/city combination | `user_location_id` |
| `core.dim_platform` | one row per `site_name × posa_continent` | `platform_id` |
| `core.dim_destination` | one row per destination ID, with latent `d1`…`d149` when available | `destination_id` |
| `core.dim_destination_type` | one row per destination type ID | `destination_type_id` |
| `core.dim_hotel_market` | one row per observed `hotel_market × hotel_country × hotel_continent` combination | `hotel_market_id` |
| `core.dim_hotel_cluster` | one row per hotel cluster | `hotel_cluster_id` |
| `core.dim_search_params` | one row per parameter combination: adults, children, rooms, stay nights, party size, children flag | `search_params_id` |
| `core.fct_event` | one unique aggregated source log row after exact deduplication | `event_id` |
| `core.fct_booking` | one train booking log event | `booking_id` |
| `core.ref_distance_stats` | one median estimator per hierarchy group | composite group key |

`dim_user_location` is present because `985,538` user IDs have multiple observed location combinations. `hotel_market` is keyed by the actual attribute combination because `57` market IDs violate the one-to-one country/continent mapping.

## Fact semantics

`fct_event` is not a click, search, session, or booking journey. `cnt` remains the multiplicity of the aggregated source row. `search_params_id` is only a surrogate for a parameter combination and is never a search/session identifier. `hotel_cluster_id` is a cluster, not a hotel ID.

`booking_value_proxy` is 0 for non-bookings, 1 for hotel-only bookings, and 2 for package bookings; it is not money or revenue. `fct_booking` filters `is_booking = 1` and therefore contains train observations only.

## Quality and validity

STAGING preserves source grain and source values, including NULL distance. It adds date parsing, duplicate metadata, and quality flags. The active project-date range is inclusive from `2013-01-01` through `2016-12-31` for event, check-in, and check-out dates. The legacy `q_extreme_future_date` field flags any present date outside that range. CORE keeps the first row of each exact source-payload duplicate group using deterministic `source_row_id` order. Suspicious records are not removed for quality reasons: out-of-range dates receive no `dim_date` key, and derived date metrics remain NULL. `lead_days` and `stay_nights` are populated only under their validity flags; same-day stays are excluded from `valid_for_stay_length` because their business meaning is ambiguous.

## Distance

`distance_raw` is immutable source distance. Missing values are filled only in CORE using median estimators and minimum support `5` in this order: city×destination, city×hotel market, region×destination, region×hotel market. The three country-level candidates are measured in the holdout but not applied because their errors are materially broader. Provenance and holdout validation metrics are stored in `fct_event` and `ref_distance_stats`.

## Validation snapshot

- RAW rows: `40,198,536` (`train=37,670,293`, `test=2,528,243`)
- STAGING interaction rows: `40,198,536`
- exact-duplicate rows flagged in STAGING: `1,922`
- CORE event rows: `40,197,567`
- rows removed by controlled exact deduplication: `969`
- fct_event fan-out check: `PASS`
