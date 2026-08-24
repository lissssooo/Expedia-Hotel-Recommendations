"""Validate the 14 CSV marts delivered to DataLens."""

from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MARTS_DIR = ROOT / "data" / "marts"

EXPECTED = {
    "mart_booking_frequency.csv": (
        5,
        "booking_count_bucket,booking_count_bucket_order,users,user_share,"
        "avg_sessions,avg_active_months,avg_booking_value_proxy,"
        "avg_package_booking_share",
    ),
    "mart_booking_frequency_exact.csv": (
        101,
        "bookings,users,user_share",
    ),
    "mart_channel_platform.csv": (
        11720,
        "year_month,channel,platform_id,is_mobile,active_users,row_events,"
        "weighted_events,bookings,booking_row_rate,booking_weighted_event_rate,"
        "booking_value_proxy_total,booking_value_proxy_per_active_user,"
        "package_booking_share,avg_valid_lead_days,avg_valid_stay_nights",
    ),
    "mart_data_quality_daily.csv": (
        724,
        "date_key,rows,weighted_events,missing_distance_share,"
        "imputed_distance_share,invalid_lead_time_share,invalid_stay_share,"
        "zero_party_share,quality_issue_share",
    ),
    "mart_destination_performance.csv": (
        502728,
        "year_month,destination_id,hotel_market_id,active_users,row_events,"
        "weighted_events,bookings,bookers,booking_row_rate,"
        "booking_weighted_event_rate,package_booking_share,"
        "booking_value_proxy_total,avg_distance_filled,avg_valid_lead_days,"
        "avg_valid_stay_nights,meets_min_volume_flag,"
        "meets_booking_min_volume_flag",
    ),
    "mart_distance_quality.csv": (
        49,
        "imputation_level,min_support,holdout_rows,covered_rows,coverage_pct,"
        "mae,median_absolute_error,p90_absolute_error,average_support",
    ),
    "mart_origin_destination.csv": (
        151998,
        "year_month,user_country,hotel_country,active_users,row_events,"
        "weighted_events,bookings,booking_row_rate,booking_value_proxy_total,"
        "avg_distance_filled,package_booking_share,avg_valid_stay_nights,"
        "avg_valid_lead_days",
    ),
    "mart_package_profile.csv": (
        72280,
        "year_month,is_package,lead_time_bucket,stay_length_bucket,"
        "party_segment,channel,is_mobile,users,events,weighted_events,bookings,"
        "weighted_bookings,booking_row_rate,booking_weighted_event_rate,"
        "booking_value_proxy_total",
    ),
    "mart_product_daily.csv": (
        724,
        "date_key,active_users,row_events,weighted_events,bookings,bookers,"
        "booking_row_rate,booking_weighted_event_rate,booker_rate,"
        "booking_value_proxy_total,booking_value_proxy_per_active_user,"
        "avg_booking_value_proxy_per_booking,mobile_row_share,"
        "mobile_booking_share,package_booking_share,avg_valid_lead_days,"
        "avg_valid_stay_nights,avg_distance_filled,distance_imputed_share",
    ),
    "mart_retention_cohort.csv": (
        300,
        "cohort_month,months_since_first_booking,cohort_users,returned_bookers,"
        "booking_retention_rate,bookings,booking_value_proxy_total",
    ),
    "mart_session_daily.csv": (
        724,
        "date_key,active_users,sessions,booking_sessions,session_booking_rate,"
        "sessions_per_user,avg_rows_per_session,"
        "avg_weighted_events_per_session,median_session_duration_seconds,"
        "avg_time_to_first_booking_seconds,multi_destination_session_share,"
        "booking_value_proxy_total,booking_value_proxy_per_session",
    ),
    "mart_travel_calendar_daily.csv": (
        6908,
        "date_key,full_date,year,month,year_month,events_on_date,"
        "weighted_events_on_date,bookings_made_on_date,checkins_on_date,"
        "checkouts_on_date,booking_value_proxy_for_checkins,package_checkins,"
        "avg_stay_nights_for_checkins,avg_lead_days_for_checkins",
    ),
    "mart_trip_profile.csv": (
        2399,
        "year_month,lead_time_bucket,stay_length_bucket,party_segment,users,"
        "events,weighted_events,bookings,booking_row_rate,"
        "booking_weighted_event_rate,package_share,mobile_share,"
        "booking_value_proxy_total,sessions,booking_sessions,"
        "session_booking_rate",
    ),
    "mart_user_360.csv": (
        1198786,
        "user_id,first_seen_date,last_seen_date,active_days,active_months,"
        "row_events,weighted_events,bookings,first_booking_date,"
        "last_booking_date,booking_value_proxy_total,avg_booking_value_proxy,"
        "package_bookings,package_booking_share,mobile_event_share,"
        "distinct_destinations,distinct_hotel_markets,avg_valid_lead_days,"
        "avg_valid_stay_nights,avg_distance_filled,sessions,booking_sessions,"
        "days_since_last_booking,booking_frequency,session_frequency,"
        "observation_end_date",
    ),
}


def read_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return next(csv.reader(handle))


def row_count(path: Path) -> int:
    with path.open("rb") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def main() -> None:
    failures: list[str] = []
    actual_files = {path.name for path in MARTS_DIR.glob("*.csv")}
    expected_files = set(EXPECTED)
    for name in sorted(expected_files - actual_files):
        failures.append(f"missing file: {name}")
    for name in sorted(actual_files - expected_files):
        failures.append(f"unexpected CSV: {name}")

    for name, (expected_rows, expected_header) in EXPECTED.items():
        path = MARTS_DIR / name
        if not path.exists():
            continue
        header = read_header(path)
        rows = row_count(path)
        if header != expected_header.split(","):
            failures.append(f"wrong header: {name}")
        if rows != expected_rows:
            failures.append(
                f"wrong row count: {name}: expected {expected_rows}, got {rows}"
            )
        print(f"ok {name}: {rows:,} rows, {len(header)} columns")

    if failures:
        raise SystemExit("\n".join(failures))
    print("All 14 marts passed validation.")


if __name__ == "__main__":
    main()
