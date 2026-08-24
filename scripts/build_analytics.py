"""Build reconstructed sessions and the analytical marts.

The script reads CORE only and writes derived CORE session objects, MARTS, and
small audit artifacts.  RAW and source-aligned Parquet files are never changed.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

try:
    from tools.duckdb_runtime import configure_duckdb, runtime_settings
except ModuleNotFoundError:  # Direct entrypoint: python3 tools/build_analytics.py
    from duckdb_runtime import configure_duckdb, runtime_settings


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "analytics.duckdb"
CORE_DIR = ROOT / "data" / "derived" / "core"
MARTS_DIR = ROOT / "data" / "derived" / "marts"
ARTIFACTS_DIR = ROOT / "artifacts"
DOCS_DIR = ROOT / "docs"
BUILD_TS = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
SESSION_RULE_VERSION = "gap_30m_v1"
SENSITIVITY_SAMPLE_MODULUS = 20  # deterministic 5% user sample
SESSION_BUCKETS = 32
SESSION_FRAGMENTS_DIR = CORE_DIR / "session_events"
SESSION_SUMMARIES_DIR = CORE_DIR / "session_summaries"


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def output_path(path: Path) -> str:
    return sql_literal(str(path.resolve()).replace("\\", "/"))


def materialize(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    name: str,
    query: str,
    directory: Path,
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.parquet"
    if path.exists():
        path.unlink()
    con.execute(
        f"COPY ({query}) TO {output_path(path)} "
        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW {schema}.{name} AS "
        f"SELECT * FROM read_parquet({output_path(path)})"
    )
    return path


def scalar(con: duckdb.DuckDBPyConnection, query: str):
    return con.execute(query).fetchone()[0]


def table_rows(con: duckdb.DuckDBPyConnection, relation: str) -> int:
    return int(scalar(con, f"SELECT COUNT(*) FROM {relation}"))


def build_session_fragments(con: duckdb.DuckDBPyConnection) -> str:
    """Sessionize one deterministic user hash bucket at a time.

    Sessions cannot cross users, so user hashing is an exact partitioning
    strategy rather than an approximation. Each fragment is small enough for
    DuckDB's window functions to spill safely instead of materializing all
    train events in one in-memory relation.
    """
    eligible_event_count = int(scalar(con, """
        SELECT COUNT(*)
        FROM core.fct_event
        WHERE source_dataset = 'train'
          AND user_id IS NOT NULL
          AND event_ts IS NOT NULL
          AND event_date_key IS NOT NULL
    """))
    core_manifest_path = ARTIFACTS_DIR / "core_manifest.json"
    core_build_timestamp = None
    if core_manifest_path.exists():
        core_build_timestamp = json.loads(
            core_manifest_path.read_text(encoding="utf-8")
        ).get("build_timestamp")
    cache_manifest_path = SESSION_FRAGMENTS_DIR / "_manifest.json"
    cache_signature = {
        "session_rule_version": SESSION_RULE_VERSION,
        "session_buckets": SESSION_BUCKETS,
        "eligible_event_count": eligible_event_count,
        "core_build_timestamp": core_build_timestamp,
    }
    existing = sorted(SESSION_FRAGMENTS_DIR.glob("part_*.parquet"))
    if (
        len(existing) == SESSION_BUCKETS
        and all(path.stat().st_size > 0 for path in existing)
        and cache_manifest_path.exists()
        and json.loads(cache_manifest_path.read_text(encoding="utf-8")) == cache_signature
    ):
        return str((SESSION_FRAGMENTS_DIR / "part_*.parquet").resolve()).replace("\\", "/")

    if SESSION_FRAGMENTS_DIR.exists():
        for path in SESSION_FRAGMENTS_DIR.glob("part_*.parquet"):
            path.unlink()
    else:
        SESSION_FRAGMENTS_DIR.mkdir(parents=True, exist_ok=True)

    for bucket in range(SESSION_BUCKETS):
        query = f"""
        WITH base AS (
            SELECT
                e.event_id,
                e.source_dataset,
                e.event_ts,
                e.event_date_key,
                e.event_hour_key,
                e.user_id,
                e.platform_id,
                e.destination_id,
                e.hotel_market_id,
                e.search_params_id,
                e.channel,
                e.is_mobile,
                e.is_package,
                e.is_booking,
                e.cnt,
                e.lead_days,
                e.stay_nights,
                e.party_size,
                e.booking_value_proxy,
                e.distance_filled,
                e.distance_is_imputed,
                e.valid_for_lead_time,
                e.valid_for_stay_length,
                e.valid_for_party_metrics
            FROM core.fct_event e
            WHERE e.source_dataset = 'train'
              AND e.user_id IS NOT NULL
              AND e.event_ts IS NOT NULL
              AND e.event_date_key IS NOT NULL
              AND hash(e.user_id) % {SESSION_BUCKETS} = {bucket}
        ), ordered AS (
            SELECT
                base.*,
                LAG(event_ts) OVER (
                    PARTITION BY user_id ORDER BY event_ts, event_id
                ) AS previous_event_ts
            FROM base
        ), marked AS (
            SELECT
                ordered.*,
                CASE
                    WHEN previous_event_ts IS NULL THEN 1
                    WHEN event_ts - previous_event_ts > INTERVAL 30 MINUTE THEN 1
                    ELSE 0
                END AS session_start_flag
            FROM ordered
        ), numbered AS (
            SELECT
                marked.*,
                SUM(session_start_flag) OVER (
                    PARTITION BY user_id
                    ORDER BY event_ts, event_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::BIGINT AS session_number
            FROM marked
        ), keyed AS (
            SELECT
                numbered.*,
                FIRST_VALUE(event_id) OVER (
                    PARTITION BY user_id, session_number
                    ORDER BY event_ts, event_id
                ) AS first_event_id,
                {sql_literal(SESSION_RULE_VERSION)}::VARCHAR AS session_rule_version
            FROM numbered
        )
        SELECT
            keyed.* EXCLUDE (previous_event_ts, session_start_flag, session_number,
                            first_event_id),
            md5(concat_ws(
                '|', session_rule_version, source_dataset,
                CAST(user_id AS VARCHAR), CAST(first_event_id AS VARCHAR)
            )) AS session_id
        FROM keyed
        """
        path = SESSION_FRAGMENTS_DIR / f"part_{bucket:02d}.parquet"
        con.execute(
            f"COPY ({query}) TO {output_path(path)} "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000)"
        )

    cache_manifest_path.write_text(
        json.dumps(cache_signature, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    return str((SESSION_FRAGMENTS_DIR / "part_*.parquet").resolve()).replace("\\", "/")


def build_sensitivity(con: duckdb.DuckDBPyConnection) -> dict:
    rows = []
    for gap_minutes in (15, 30, 60, 120):
        query = f"""
        WITH sampled AS (
            SELECT event_id, event_ts, user_id, cnt, destination_id, is_booking
            FROM core.fct_event
            WHERE source_dataset = 'train'
              AND user_id IS NOT NULL
              AND event_ts IS NOT NULL
              AND event_date_key IS NOT NULL
              AND hash(user_id) % {SENSITIVITY_SAMPLE_MODULUS} = 0
        ), ordered AS (
            SELECT sampled.*,
                LAG(event_ts) OVER (
                    PARTITION BY user_id ORDER BY event_ts, event_id
                ) AS previous_event_ts
            FROM sampled
        ), numbered AS (
            SELECT ordered.*,
                SUM(CASE WHEN previous_event_ts IS NULL
                              OR event_ts - previous_event_ts > INTERVAL {gap_minutes} MINUTE
                         THEN 1 ELSE 0 END) OVER (
                    PARTITION BY user_id ORDER BY event_ts, event_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS session_number
            FROM ordered
        ), sessions AS (
            SELECT
                user_id, session_number,
                COUNT(*)::BIGINT AS row_count,
                SUM(COALESCE(cnt, 0))::BIGINT AS weighted_event_count,
                MIN(event_ts) AS session_start_ts,
                MAX(event_ts) AS session_end_ts,
                COUNT(DISTINCT destination_id) AS distinct_destination_count,
                COUNT(*) FILTER (WHERE is_booking = 1)::BIGINT AS booking_row_count
            FROM numbered
            GROUP BY user_id, session_number
        )
        SELECT
            {gap_minutes}::INTEGER AS gap_minutes,
            COUNT(*)::BIGINT AS sessions,
            COUNT(DISTINCT user_id)::BIGINT AS active_users,
            1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT user_id), 0) AS sessions_per_active_user,
            MEDIAN(row_count) AS median_rows_per_session,
            MEDIAN(weighted_event_count) AS median_weighted_events_per_session,
            MEDIAN(date_diff('second', session_start_ts, session_end_ts))
                AS median_session_duration_seconds,
            QUANTILE_CONT(date_diff('second', session_start_ts, session_end_ts), 0.9)
                AS p90_session_duration_seconds,
            100.0 * COUNT(*) FILTER (WHERE row_count = 1) / COUNT(*)
                AS one_row_session_share_pct,
            100.0 * COUNT(*) FILTER (WHERE booking_row_count > 0) / COUNT(*)
                AS booking_session_rate_pct,
            100.0 * COUNT(*) FILTER (WHERE distinct_destination_count > 1) / COUNT(*)
                AS multi_destination_session_share_pct,
            100.0 * COUNT(*) FILTER (WHERE booking_row_count > 1) / COUNT(*)
                AS multi_booking_row_session_share_pct
        FROM sessions
        """
        rows.extend(con.execute(query).fetchdf().to_dict(orient="records"))
    import pandas as pd
    df = pd.DataFrame(rows).sort_values("gap_minutes")
    path = ARTIFACTS_DIR / "session_sensitivity.csv"
    df.to_csv(path, index=False)
    return {
        "sample_user_count": int(scalar(
            con,
            f"SELECT COUNT(DISTINCT user_id) FROM train_event_enriched "
            f"WHERE hash(user_id) % {SENSITIVITY_SAMPLE_MODULUS} = 0",
        )),
        "sample_event_count": int(scalar(
            con,
            f"SELECT COUNT(*) FROM train_event_enriched "
            f"WHERE hash(user_id) % {SENSITIVITY_SAMPLE_MODULUS} = 0",
        )),
        "rows": df.to_dict(orient="records"),
    }


def build_marts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    paths: dict[str, int] = {}

    product_daily = """
    SELECT
        event_date_key AS date_key,
        COUNT(DISTINCT user_id)::BIGINT AS active_users,
        COUNT(*)::BIGINT AS row_events,
        SUM(COALESCE(cnt, 0))::BIGINT AS weighted_events,
        COUNT(*) FILTER (WHERE is_booking = 1)::BIGINT AS bookings,
        COUNT(DISTINCT user_id) FILTER (WHERE is_booking = 1)::BIGINT AS bookers,
        1.0 * COUNT(*) FILTER (WHERE is_booking = 1) / COUNT(*) AS booking_row_rate,
        1.0 * SUM(CASE WHEN is_booking = 1 THEN COALESCE(cnt, 0) ELSE 0 END)
            / NULLIF(SUM(COALESCE(cnt, 0)), 0) AS booking_weighted_event_rate,
        1.0 * COUNT(DISTINCT user_id) FILTER (WHERE is_booking = 1)
            / NULLIF(COUNT(DISTINCT user_id), 0) AS booker_rate,
        SUM(CASE WHEN is_booking = 1 THEN COALESCE(booking_value_proxy, 0) ELSE 0 END)::BIGINT
            AS booking_value_proxy_total,
        1.0 * SUM(CASE WHEN is_booking = 1 THEN COALESCE(booking_value_proxy, 0) ELSE 0 END)
            / NULLIF(COUNT(DISTINCT user_id), 0) AS booking_value_proxy_per_active_user,
        1.0 * SUM(CASE WHEN is_booking = 1 THEN COALESCE(booking_value_proxy, 0) ELSE 0 END)
            / NULLIF(COUNT(*) FILTER (WHERE is_booking = 1), 0)
            AS avg_booking_value_proxy_per_booking,
        1.0 * COUNT(*) FILTER (WHERE is_mobile = 1) / COUNT(*) AS mobile_row_share,
        1.0 * COUNT(*) FILTER (WHERE is_booking = 1 AND is_mobile = 1)
            / NULLIF(COUNT(*) FILTER (WHERE is_booking = 1), 0) AS mobile_booking_share,
        1.0 * COUNT(*) FILTER (WHERE is_booking = 1 AND is_package = 1)
            / NULLIF(COUNT(*) FILTER (WHERE is_booking = 1), 0) AS package_booking_share,
        AVG(lead_days) FILTER (WHERE valid_for_lead_time) AS avg_valid_lead_days,
        AVG(stay_nights) FILTER (WHERE valid_for_stay_length) AS avg_valid_stay_nights,
        AVG(distance_filled) AS avg_distance_filled,
        1.0 * COUNT(*) FILTER (WHERE distance_is_imputed) / COUNT(*) AS distance_imputed_share
    FROM train_event_enriched
    GROUP BY event_date_key
    """
    materialize(con, "marts", "mart_product_daily", product_daily, MARTS_DIR)
    paths["mart_product_daily"] = table_rows(con, "marts.mart_product_daily")

    session_daily = f"""
    SELECT
        session_date_key AS date_key,
        COUNT(DISTINCT user_id)::BIGINT AS active_users,
        COUNT(*)::BIGINT AS sessions,
        COUNT(*) FILTER (WHERE has_booking)::BIGINT AS booking_sessions,
        1.0 * COUNT(*) FILTER (WHERE has_booking) / COUNT(*) AS session_booking_rate,
        1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT user_id), 0) AS sessions_per_user,
        AVG(row_count) AS avg_rows_per_session,
        AVG(weighted_event_count) AS avg_weighted_events_per_session,
        MEDIAN(session_duration_seconds) AS median_session_duration_seconds,
        AVG(time_to_first_booking_seconds)
            FILTER (WHERE has_booking) AS avg_time_to_first_booking_seconds,
        1.0 * COUNT(*) FILTER (WHERE distinct_destination_count > 1) / COUNT(*)
            AS multi_destination_session_share,
        SUM(booking_value_proxy_total)::BIGINT AS booking_value_proxy_total,
        1.0 * SUM(booking_value_proxy_total) / COUNT(*)
            AS booking_value_proxy_per_session
    FROM core.fct_session
    WHERE session_rule_version = {sql_literal(SESSION_RULE_VERSION)}
      AND source_dataset = 'train'
    GROUP BY session_date_key
    """
    materialize(con, "marts", "mart_session_daily", session_daily, MARTS_DIR)
    paths["mart_session_daily"] = table_rows(con, "marts.mart_session_daily")

    travel_calendar = """
    WITH event_daily AS (
        SELECT
            event_date_key AS date_key,
            COUNT(*)::BIGINT AS events_on_date,
            SUM(COALESCE(cnt, 0))::BIGINT AS weighted_events_on_date,
            COUNT(*) FILTER (WHERE is_booking = 1)::BIGINT AS bookings_made_on_date
        FROM train_event_enriched
        GROUP BY event_date_key
    ), checkins AS (
        SELECT
            checkin_date_key AS date_key,
            COUNT(*)::BIGINT AS checkins_on_date,
            SUM(COALESCE(booking_value_proxy, 0))::BIGINT
                AS booking_value_proxy_for_checkins,
            COUNT(*) FILTER (WHERE is_package = 1)::BIGINT AS package_checkins,
            AVG(stay_nights) FILTER (WHERE valid_for_stay_length) AS avg_stay_nights_for_checkins,
            AVG(lead_days) FILTER (WHERE valid_for_lead_time) AS avg_lead_days_for_checkins
        FROM train_event_enriched
        WHERE is_booking = 1 AND checkin_date_key IS NOT NULL
        GROUP BY checkin_date_key
    ), checkouts AS (
        SELECT checkin_date_key, checkout_date_key
        FROM train_event_enriched
        WHERE is_booking = 1 AND checkout_date_key IS NOT NULL
    ), checkout_daily AS (
        SELECT checkout_date_key AS date_key, COUNT(*)::BIGINT AS checkouts_on_date
        FROM checkouts
        GROUP BY checkout_date_key
    )
    SELECT
        d.date_key,
        d.full_date,
        d.year,
        d.month,
        d.year_month,
        COALESCE(e.events_on_date, 0)::BIGINT AS events_on_date,
        COALESCE(e.weighted_events_on_date, 0)::BIGINT AS weighted_events_on_date,
        COALESCE(e.bookings_made_on_date, 0)::BIGINT AS bookings_made_on_date,
        COALESCE(c.checkins_on_date, 0)::BIGINT AS checkins_on_date,
        COALESCE(x.checkouts_on_date, 0)::BIGINT AS checkouts_on_date,
        COALESCE(c.booking_value_proxy_for_checkins, 0)::BIGINT
            AS booking_value_proxy_for_checkins,
        COALESCE(c.package_checkins, 0)::BIGINT AS package_checkins,
        c.avg_stay_nights_for_checkins,
        c.avg_lead_days_for_checkins
    FROM core.dim_date d
    LEFT JOIN event_daily e USING (date_key)
    LEFT JOIN checkins c USING (date_key)
    LEFT JOIN checkout_daily x USING (date_key)
    """
    materialize(con, "marts", "mart_travel_calendar_daily", travel_calendar, MARTS_DIR)
    paths["mart_travel_calendar_daily"] = table_rows(con, "marts.mart_travel_calendar_daily")

    channel_platform = """
    SELECT
        year_month,
        channel,
        platform_id,
        is_mobile,
        COUNT(DISTINCT user_id)::BIGINT AS active_users,
        COUNT(*)::BIGINT AS row_events,
        SUM(COALESCE(cnt, 0))::BIGINT AS weighted_events,
        COUNT(*) FILTER (WHERE is_booking = 1)::BIGINT AS bookings,
        1.0 * COUNT(*) FILTER (WHERE is_booking = 1) / COUNT(*) AS booking_row_rate,
        1.0 * SUM(CASE WHEN is_booking = 1 THEN COALESCE(cnt, 0) ELSE 0 END)
            / NULLIF(SUM(COALESCE(cnt, 0)), 0) AS booking_weighted_event_rate,
        SUM(CASE WHEN is_booking = 1 THEN COALESCE(booking_value_proxy, 0) ELSE 0 END)::BIGINT
            AS booking_value_proxy_total,
        1.0 * SUM(CASE WHEN is_booking = 1 THEN COALESCE(booking_value_proxy, 0) ELSE 0 END)
            / NULLIF(COUNT(DISTINCT user_id), 0) AS booking_value_proxy_per_active_user,
        1.0 * COUNT(*) FILTER (WHERE is_booking = 1 AND is_package = 1)
            / NULLIF(COUNT(*) FILTER (WHERE is_booking = 1), 0) AS package_booking_share,
        AVG(lead_days) FILTER (WHERE valid_for_lead_time) AS avg_valid_lead_days,
        AVG(stay_nights) FILTER (WHERE valid_for_stay_length) AS avg_valid_stay_nights
    FROM train_event_enriched
    GROUP BY year_month, channel, platform_id, is_mobile
    """
    materialize(con, "marts", "mart_channel_platform", channel_platform, MARTS_DIR)
    paths["mart_channel_platform"] = table_rows(con, "marts.mart_channel_platform")

    destination_performance = """
    SELECT
        year_month,
        destination_id,
        hotel_market_id,
        COUNT(DISTINCT user_id)::BIGINT AS active_users,
        COUNT(*)::BIGINT AS row_events,
        SUM(COALESCE(cnt, 0))::BIGINT AS weighted_events,
        COUNT(*) FILTER (WHERE is_booking = 1)::BIGINT AS bookings,
        COUNT(DISTINCT user_id) FILTER (WHERE is_booking = 1)::BIGINT AS bookers,
        1.0 * COUNT(*) FILTER (WHERE is_booking = 1) / COUNT(*) AS booking_row_rate,
        1.0 * SUM(CASE WHEN is_booking = 1 THEN COALESCE(cnt, 0) ELSE 0 END)
            / NULLIF(SUM(COALESCE(cnt, 0)), 0) AS booking_weighted_event_rate,
        1.0 * COUNT(*) FILTER (WHERE is_booking = 1 AND is_package = 1)
            / NULLIF(COUNT(*) FILTER (WHERE is_booking = 1), 0) AS package_booking_share,
        SUM(CASE WHEN is_booking = 1 THEN COALESCE(booking_value_proxy, 0) ELSE 0 END)::BIGINT
            AS booking_value_proxy_total,
        AVG(distance_filled) AS avg_distance_filled,
        AVG(lead_days) FILTER (WHERE valid_for_lead_time) AS avg_valid_lead_days,
        AVG(stay_nights) FILTER (WHERE valid_for_stay_length) AS avg_valid_stay_nights,
        COUNT(*) >= 100 AS meets_min_volume_flag,
        COUNT(*) FILTER (WHERE is_booking = 1) >= 10 AS meets_booking_min_volume_flag
    FROM train_event_enriched
    GROUP BY year_month, destination_id, hotel_market_id
    """
    materialize(con, "marts", "mart_destination_performance", destination_performance, MARTS_DIR)
    paths["mart_destination_performance"] = table_rows(con, "marts.mart_destination_performance")

    user_360 = f"""
    WITH observation AS (
        SELECT MAX(CAST(event_ts AS DATE)) AS observation_end_date
        FROM train_event_enriched
    ), events AS (
        SELECT
            user_id,
            MIN(CAST(event_ts AS DATE)) AS first_seen_date,
            MAX(CAST(event_ts AS DATE)) AS last_seen_date,
            COUNT(DISTINCT CAST(event_ts AS DATE))::BIGINT AS active_days,
            COUNT(DISTINCT year_month)::BIGINT AS active_months,
            COUNT(*)::BIGINT AS row_events,
            SUM(COALESCE(cnt, 0))::BIGINT AS weighted_events,
            COUNT(*) FILTER (WHERE is_booking = 1)::BIGINT AS bookings,
            MIN(CAST(event_ts AS DATE)) FILTER (WHERE is_booking = 1) AS first_booking_date,
            MAX(CAST(event_ts AS DATE)) FILTER (WHERE is_booking = 1) AS last_booking_date,
            SUM(CASE WHEN is_booking = 1 THEN COALESCE(booking_value_proxy, 0) ELSE 0 END)::BIGINT
                AS booking_value_proxy_total,
            AVG(booking_value_proxy) FILTER (WHERE is_booking = 1)
                AS avg_booking_value_proxy,
            COUNT(*) FILTER (WHERE is_booking = 1 AND is_package = 1)::BIGINT AS package_bookings,
            1.0 * COUNT(*) FILTER (WHERE is_booking = 1 AND is_package = 1)
                / NULLIF(COUNT(*) FILTER (WHERE is_booking = 1), 0) AS package_booking_share,
            1.0 * COUNT(*) FILTER (WHERE is_mobile = 1) / COUNT(*) AS mobile_event_share,
            COUNT(DISTINCT destination_id)::BIGINT AS distinct_destinations,
            COUNT(DISTINCT hotel_market_id)::BIGINT AS distinct_hotel_markets,
            AVG(lead_days) FILTER (WHERE valid_for_lead_time) AS avg_valid_lead_days,
            AVG(stay_nights) FILTER (WHERE valid_for_stay_length) AS avg_valid_stay_nights,
            AVG(distance_filled) AS avg_distance_filled
        FROM train_event_enriched
        GROUP BY user_id
    ), sessions AS (
        SELECT
            user_id,
            COUNT(*)::BIGINT AS sessions,
            COUNT(*) FILTER (WHERE has_booking)::BIGINT AS booking_sessions
        FROM core.fct_session
        WHERE source_dataset = 'train' AND session_rule_version = {sql_literal(SESSION_RULE_VERSION)}
        GROUP BY user_id
    )
    SELECT
        e.*,
        COALESCE(s.sessions, 0)::BIGINT AS sessions,
        COALESCE(s.booking_sessions, 0)::BIGINT AS booking_sessions,
        date_diff('day', e.last_booking_date, o.observation_end_date)
            AS days_since_last_booking,
        1.0 * e.bookings / NULLIF(e.active_months, 0) AS booking_frequency,
        1.0 * COALESCE(s.sessions, 0) / NULLIF(e.active_months, 0) AS session_frequency,
        o.observation_end_date
    FROM events e
    LEFT JOIN sessions s USING (user_id)
    CROSS JOIN observation o
    """
    materialize(con, "marts", "mart_user_360", user_360, MARTS_DIR)
    paths["mart_user_360"] = table_rows(con, "marts.mart_user_360")

    origin_destination = """
    SELECT
        year_month,
        user_country,
        hotel_country,
        COUNT(DISTINCT user_id)::BIGINT AS active_users,
        COUNT(*)::BIGINT AS row_events,
        SUM(COALESCE(cnt, 0))::BIGINT AS weighted_events,
        COUNT(*) FILTER (WHERE is_booking = 1)::BIGINT AS bookings,
        1.0 * COUNT(*) FILTER (WHERE is_booking = 1) / COUNT(*) AS booking_row_rate,
        SUM(CASE WHEN is_booking = 1 THEN COALESCE(booking_value_proxy, 0) ELSE 0 END)::BIGINT
            AS booking_value_proxy_total,
        AVG(distance_filled) AS avg_distance_filled,
        1.0 * COUNT(*) FILTER (WHERE is_booking = 1 AND is_package = 1)
            / NULLIF(COUNT(*) FILTER (WHERE is_booking = 1), 0) AS package_booking_share,
        AVG(stay_nights) FILTER (WHERE valid_for_stay_length) AS avg_valid_stay_nights,
        AVG(lead_days) FILTER (WHERE valid_for_lead_time) AS avg_valid_lead_days
    FROM train_event_enriched
    GROUP BY year_month, user_country, hotel_country
    """
    materialize(con, "marts", "mart_origin_destination", origin_destination, MARTS_DIR)
    paths["mart_origin_destination"] = table_rows(con, "marts.mart_origin_destination")

    trip_profile = f"""
    WITH profiled AS (
        SELECT
            e.*,
            CASE
                WHEN lead_days BETWEEN 0 AND 1 THEN 'same_next_day'
                WHEN lead_days BETWEEN 2 AND 7 THEN '2_7'
                WHEN lead_days BETWEEN 8 AND 30 THEN '8_30'
                WHEN lead_days BETWEEN 31 AND 90 THEN '31_90'
                WHEN lead_days >= 91 THEN '91_plus'
            END AS lead_time_bucket,
            CASE
                WHEN stay_nights = 1 THEN '1'
                WHEN stay_nights BETWEEN 2 AND 3 THEN '2_3'
                WHEN stay_nights BETWEEN 4 AND 7 THEN '4_7'
                WHEN stay_nights BETWEEN 8 AND 14 THEN '8_14'
                WHEN stay_nights >= 15 THEN '15_plus'
            END AS stay_length_bucket,
            CASE
                WHEN party_size = 1 THEN 'solo'
                WHEN adults_cnt = 2 AND COALESCE(children_cnt, 0) = 0 THEN 'couple'
                WHEN COALESCE(children_cnt, 0) > 0 THEN 'family_with_children'
                WHEN party_size > 0 THEN 'group'
            END AS party_segment
        FROM train_event_enriched e
        WHERE valid_for_lead_time AND valid_for_stay_length AND valid_for_party_metrics
    ), with_sessions AS (
        SELECT
            p.*,
            m.session_id,
            s.has_booking AS session_has_booking
        FROM profiled p
        LEFT JOIN core.event_session_map m
          ON m.event_id = p.event_id
         AND m.session_rule_version = {sql_literal(SESSION_RULE_VERSION)}
        LEFT JOIN core.fct_session s ON s.session_id = m.session_id
    )
    SELECT
        year_month,
        lead_time_bucket,
        stay_length_bucket,
        party_segment,
        COUNT(DISTINCT user_id)::BIGINT AS users,
        COUNT(*)::BIGINT AS events,
        SUM(COALESCE(cnt, 0))::BIGINT AS weighted_events,
        COUNT(*) FILTER (WHERE is_booking = 1)::BIGINT AS bookings,
        1.0 * COUNT(*) FILTER (WHERE is_booking = 1) / COUNT(*) AS booking_row_rate,
        1.0 * SUM(CASE WHEN is_booking = 1 THEN COALESCE(cnt, 0) ELSE 0 END)
            / NULLIF(SUM(COALESCE(cnt, 0)), 0) AS booking_weighted_event_rate,
        1.0 * COUNT(*) FILTER (WHERE is_booking = 1 AND is_package = 1)
            / NULLIF(COUNT(*) FILTER (WHERE is_booking = 1), 0) AS package_share,
        1.0 * COUNT(*) FILTER (WHERE is_mobile = 1) / COUNT(*) AS mobile_share,
        SUM(CASE WHEN is_booking = 1 THEN COALESCE(booking_value_proxy, 0) ELSE 0 END)::BIGINT
            AS booking_value_proxy_total,
        COUNT(DISTINCT session_id)::BIGINT AS sessions,
        COUNT(DISTINCT session_id) FILTER (WHERE session_has_booking)::BIGINT AS booking_sessions,
        1.0 * COUNT(DISTINCT session_id) FILTER (WHERE session_has_booking)
            / NULLIF(COUNT(DISTINCT session_id), 0) AS session_booking_rate
    FROM with_sessions
    GROUP BY year_month, lead_time_bucket, stay_length_bucket, party_segment
    """
    materialize(con, "marts", "mart_trip_profile", trip_profile, MARTS_DIR)
    paths["mart_trip_profile"] = table_rows(con, "marts.mart_trip_profile")

    package_profile = """
    WITH profiled AS (
        SELECT
            e.*,
            CASE
                WHEN lead_days BETWEEN 0 AND 1 THEN 'same_next_day'
                WHEN lead_days BETWEEN 2 AND 7 THEN '2_7'
                WHEN lead_days BETWEEN 8 AND 30 THEN '8_30'
                WHEN lead_days BETWEEN 31 AND 90 THEN '31_90'
                WHEN lead_days >= 91 THEN '91_plus'
            END AS lead_time_bucket,
            CASE
                WHEN stay_nights = 1 THEN '1'
                WHEN stay_nights BETWEEN 2 AND 3 THEN '2_3'
                WHEN stay_nights BETWEEN 4 AND 7 THEN '4_7'
                WHEN stay_nights BETWEEN 8 AND 14 THEN '8_14'
                WHEN stay_nights >= 15 THEN '15_plus'
            END AS stay_length_bucket,
            CASE
                WHEN party_size = 1 THEN 'solo'
                WHEN adults_cnt = 2 AND COALESCE(children_cnt, 0) = 0 THEN 'couple'
                WHEN COALESCE(children_cnt, 0) > 0 THEN 'family_with_children'
                WHEN party_size > 0 THEN 'group'
            END AS party_segment
        FROM train_event_enriched e
        WHERE valid_for_lead_time AND valid_for_stay_length AND valid_for_party_metrics
    )
    SELECT
        year_month,
        is_package,
        lead_time_bucket,
        stay_length_bucket,
        party_segment,
        channel,
        is_mobile,
        COUNT(DISTINCT user_id)::BIGINT AS users,
        COUNT(*)::BIGINT AS events,
        SUM(COALESCE(cnt, 0))::BIGINT AS weighted_events,
        COUNT(*) FILTER (WHERE is_booking = 1)::BIGINT AS bookings,
        SUM(CASE WHEN is_booking = 1 THEN COALESCE(cnt, 0) ELSE 0 END)::BIGINT
            AS weighted_bookings,
        1.0 * COUNT(*) FILTER (WHERE is_booking = 1) / COUNT(*) AS booking_row_rate,
        1.0 * SUM(CASE WHEN is_booking = 1 THEN COALESCE(cnt, 0) ELSE 0 END)
            / NULLIF(SUM(COALESCE(cnt, 0)), 0) AS booking_weighted_event_rate,
        SUM(CASE WHEN is_booking = 1 THEN COALESCE(booking_value_proxy, 0) ELSE 0 END)::BIGINT
            AS booking_value_proxy_total
    FROM profiled
    GROUP BY
        year_month, is_package, lead_time_bucket, stay_length_bucket,
        party_segment, channel, is_mobile
    """
    materialize(con, "marts", "mart_package_profile", package_profile, MARTS_DIR)
    paths["mart_package_profile"] = table_rows(con, "marts.mart_package_profile")

    retention = """
    WITH bookings AS (
        SELECT user_id, CAST(event_ts AS DATE) AS booking_date,
               date_trunc('month', CAST(event_ts AS DATE))::DATE AS booking_month,
               COALESCE(booking_value_proxy, 0) AS booking_value_proxy
        FROM train_event_enriched
        WHERE is_booking = 1
    ), firsts AS (
        SELECT user_id, MIN(booking_month) AS cohort_month
        FROM bookings
        GROUP BY user_id
    ), periods AS (
        SELECT
            f.cohort_month,
            date_diff('month', f.cohort_month, b.booking_month)::BIGINT
                AS months_since_first_booking,
            f.user_id,
            b.booking_value_proxy
        FROM bookings b
        JOIN firsts f USING (user_id)
    ), cohort_sizes AS (
        SELECT cohort_month, COUNT(*)::BIGINT AS cohort_users
        FROM firsts
        GROUP BY cohort_month
    )
    SELECT
        p.cohort_month,
        p.months_since_first_booking,
        c.cohort_users,
        COUNT(DISTINCT p.user_id)::BIGINT AS returned_bookers,
        1.0 * COUNT(DISTINCT p.user_id) / c.cohort_users AS booking_retention_rate,
        COUNT(*)::BIGINT AS bookings,
        SUM(p.booking_value_proxy)::BIGINT AS booking_value_proxy_total
    FROM periods p
    JOIN cohort_sizes c USING (cohort_month)
    GROUP BY p.cohort_month, p.months_since_first_booking, c.cohort_users
    """
    materialize(con, "marts", "mart_retention_cohort", retention, MARTS_DIR)
    paths["mart_retention_cohort"] = table_rows(con, "marts.mart_retention_cohort")

    booking_frequency = """
    SELECT
        CASE
            WHEN bookings = 0 THEN '0'
            WHEN bookings = 1 THEN '1'
            WHEN bookings = 2 THEN '2'
            WHEN bookings = 3 THEN '3'
            ELSE '4_plus'
        END AS booking_count_bucket,
        CASE
            WHEN bookings = 0 THEN 0
            WHEN bookings = 1 THEN 1
            WHEN bookings = 2 THEN 2
            WHEN bookings = 3 THEN 3
            ELSE 4
        END AS booking_count_bucket_order,
        COUNT(*)::BIGINT AS users,
        1.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS user_share,
        AVG(sessions) AS avg_sessions,
        AVG(active_months) AS avg_active_months,
        AVG(booking_value_proxy_total) AS avg_booking_value_proxy,
        AVG(package_booking_share) AS avg_package_booking_share
    FROM marts.mart_user_360
    GROUP BY 1, 2
    """
    materialize(con, "marts", "mart_booking_frequency", booking_frequency, MARTS_DIR)
    paths["mart_booking_frequency"] = table_rows(con, "marts.mart_booking_frequency")

    booking_frequency_exact = """
    SELECT
        CAST(COALESCE(bookings, 0) AS UBIGINT) AS bookings,
        COUNT(*)::BIGINT AS users,
        1.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS user_share
    FROM marts.mart_user_360
    GROUP BY 1
    ORDER BY 1
    """
    materialize(
        con,
        "marts",
        "mart_booking_frequency_exact",
        booking_frequency_exact,
        MARTS_DIR,
    )
    paths["mart_booking_frequency_exact"] = table_rows(
        con, "marts.mart_booking_frequency_exact"
    )

    quality_daily = """
    SELECT
        e.event_date_key AS date_key,
        COUNT(*)::BIGINT AS rows,
        SUM(COALESCE(cnt, 0))::BIGINT AS weighted_events,
        1.0 * COUNT(*) FILTER (WHERE distance_raw IS NULL) / COUNT(*)
            AS missing_distance_share,
        1.0 * COUNT(*) FILTER (WHERE distance_is_imputed) / COUNT(*)
            AS imputed_distance_share,
        1.0 * COUNT(*) FILTER (WHERE NOT valid_for_lead_time) / COUNT(*)
            AS invalid_lead_time_share,
        1.0 * COUNT(*) FILTER (WHERE NOT valid_for_stay_length) / COUNT(*)
            AS invalid_stay_share,
        1.0 * COUNT(*) FILTER (WHERE party_size = 0) / COUNT(*) AS zero_party_share,
        1.0 * COUNT(*) FILTER (WHERE quality_issue_count > 0) / COUNT(*)
            AS quality_issue_share
    FROM train_event_enriched e
    GROUP BY e.event_date_key
    """
    materialize(con, "marts", "mart_data_quality_daily", quality_daily, MARTS_DIR)
    paths["mart_data_quality_daily"] = table_rows(con, "marts.mart_data_quality_daily")

    distance_quality = f"""
    SELECT *
    FROM read_parquet({output_path(CORE_DIR / 'distance_validation.parquet')})
    """
    materialize(con, "marts", "mart_distance_quality", distance_quality, MARTS_DIR)
    paths["mart_distance_quality"] = table_rows(con, "marts.mart_distance_quality")

    return paths


def validate(con: duckdb.DuckDBPyConnection) -> dict:
    checks = {
        "event_session_map_rows": table_rows(con, "core.event_session_map"),
        "eligible_train_events": table_rows(con, "train_event_enriched"),
        "session_rows": table_rows(con, "core.fct_session"),
        "session_map_duplicate_keys": int(scalar(con, """
            SELECT COUNT(*) FROM (
                SELECT event_id, session_rule_version
                FROM core.event_session_map
                GROUP BY event_id, session_rule_version
                HAVING COUNT(*) > 1
            )
        """)),
        "session_map_orphan_events": int(scalar(con, """
            SELECT COUNT(*)
            FROM core.event_session_map m
            LEFT JOIN train_event_enriched e ON e.event_id = m.event_id
            WHERE e.event_id IS NULL
        """)),
        "session_user_violations": int(scalar(con, """
            SELECT COUNT(*) FROM (
                SELECT session_id, COUNT(DISTINCT user_id) AS users
                FROM core.fct_session
                GROUP BY session_id
                HAVING COUNT(DISTINCT user_id) <> 1
            )
        """)),
        "negative_session_durations": int(scalar(con, """
            SELECT COUNT(*) FROM core.fct_session
            WHERE session_duration_seconds < 0
        """)),
        "session_row_count_mismatch": int(scalar(con, """
            SELECT ABS(
                (SELECT SUM(row_count) FROM core.fct_session)
                - (SELECT COUNT(*) FROM train_event_enriched)
            )
        """)),
        "session_weighted_count_mismatch": int(scalar(con, """
            SELECT ABS(
                (SELECT SUM(weighted_event_count) FROM core.fct_session)
                - (SELECT SUM(COALESCE(cnt, 0)) FROM train_event_enriched)
            )
        """)),
        "session_booking_row_mismatch": int(scalar(con, """
            SELECT ABS(
                (SELECT SUM(booking_row_count) FROM core.fct_session)
                - (SELECT COUNT(*) FROM train_event_enriched WHERE is_booking = 1)
            )
        """)),
        "session_first_after_last_violations": int(scalar(con, """
            SELECT COUNT(*) FROM core.fct_session
            WHERE session_start_ts > session_end_ts
        """)),
    }
    checks["pass"] = all(value == 0 for key, value in checks.items() if key.endswith("violations") or key.endswith("mismatch") or key.endswith("orphan_events") or key.endswith("duplicate_keys")) and checks["event_session_map_rows"] == checks["eligible_train_events"]
    return checks


def write_docs_and_manifest(
    con: duckdb.DuckDBPyConnection,
    sensitivity: dict,
    mart_counts: dict[str, int],
    validation: dict,
) -> None:
    runtime_threads, runtime_memory_limit = runtime_settings()
    session_summary = con.execute("""
        SELECT
            COUNT(*) AS sessions,
            COUNT(DISTINCT user_id) AS users,
            COUNT(*) FILTER (WHERE has_booking) AS booking_sessions,
            COUNT(*) FILTER (WHERE row_count = 1) AS one_row_sessions,
            MAX(session_duration_seconds) AS max_duration_seconds,
            MEDIAN(session_duration_seconds) AS p50_duration_seconds,
            QUANTILE_CONT(session_duration_seconds, 0.9) AS p90_duration_seconds,
            QUANTILE_CONT(session_duration_seconds, 0.99) AS p99_duration_seconds
        FROM core.fct_session
    """).fetchone()

    report = f"""# Sessionization and MARTS build report

Build timestamp: `{BUILD_TS}`
Session rule: `{SESSION_RULE_VERSION}`
Source population: `source_dataset = 'train'`, with non-null `user_id`, `event_ts`, and `event_date_key`.
Sessionization is an analytical reconstruction, not the source Expedia session ID.
The build uses 32 deterministic user-hash buckets and DuckDB spill-to-disk.
This run used {runtime_threads} thread(s) and a {runtime_memory_limit} memory
limit; it never creates one in-memory train table.

## Session rule

Rows are ordered per user by `event_ts, event_id`. A new session starts when the
inactivity gap is strictly greater than 30 minutes. Same-timestamp rows remain
together. `cnt` affects weighted activity metrics only; it never affects session
boundaries. Destination, channel, and search-parameter changes do not split a
session.

## Session snapshot

| metric | value |
|---|---:|
| eligible events | {validation['eligible_train_events']:,} |
| sessions | {int(session_summary[0]):,} |
| users | {int(session_summary[1]):,} |
| booking sessions | {int(session_summary[2]):,} |
| one-row sessions | {int(session_summary[3]):,} |
| maximum duration, seconds | {int(session_summary[4]):,} |
| p50 duration, seconds | {int(session_summary[5]):,} |
| p90 duration, seconds | {int(session_summary[6]):,} |
| p99 duration, seconds | {int(session_summary[7]):,} |

## Sensitivity

The comparison in `artifacts/session_sensitivity.csv` uses a deterministic 5%
user sample ({sensitivity['sample_user_count']:,} users and
{sensitivity['sample_event_count']:,} events). It is diagnostic only; the
materialized version remains `gap_30m_v1`.

## Metric definitions

- Row events are `COUNT(*)`; weighted events are `SUM(cnt)`.
- Booking rates use booking rows or weighted booking events as named in each mart.
- Booking value proxy is 0 for non-bookings, 1 for hotel-only bookings, and 2 for package bookings; it is not money.
- `mart_product_daily`, channel, destination, origin, and trip marts use train interaction rows with a valid project event date only.
- The active project-date range is `2013-01-01` through `2016-12-31` inclusive; events outside it remain in CORE but are excluded from sessions and behavioral marts.
- `mart_travel_calendar_daily` uses valid project event dates and booking rows with valid check-in/check-out date keys.
- `mart_user_360` includes all observed train users; `observation_end_date` is the maximum observed train event date.
- `mart_retention_cohort` is observed repeat-booking behavior from each user's first observed booking, not lifetime retention.
- The destination performance minimum flags are `row_events >= 100` and `bookings >= 10`.
- `mart_trip_profile` excludes rows with invalid lead time, stay length, or party metrics. Buckets are fixed in `tools/build_analytics.py`.

## Validation

```json
{json.dumps(validation, ensure_ascii=False, indent=2)}
```

## Materialized MART row counts

| mart | rows |
|---|---:|
""" + "\n".join(
        f"| `{name}` | {count:,} |" for name, count in sorted(mart_counts.items())
    ) + "\n"
    (DOCS_DIR / "analytics_build_report.md").write_text(report, encoding="utf-8")

    manifest = {
        "manifest_version": 1,
        "build_timestamp": BUILD_TS,
        "session_rule_version": SESSION_RULE_VERSION,
        "source_population": {
            "source_dataset": "train",
            "filters": [
                "user_id IS NOT NULL",
                "event_ts IS NOT NULL",
                "event_date_key IS NOT NULL",
            ],
        },
        "runtime": {
            "duckdb_threads": runtime_threads,
            "duckdb_memory_limit": runtime_memory_limit,
        },
        "validation": validation,
        "session_summary": {
            "eligible_events": validation["eligible_train_events"],
            "sessions": int(session_summary[0]),
            "users": int(session_summary[1]),
            "booking_sessions": int(session_summary[2]),
            "one_row_sessions": int(session_summary[3]),
            "max_duration_seconds": int(session_summary[4]),
            "p50_duration_seconds": float(session_summary[5]),
            "p90_duration_seconds": float(session_summary[6]),
            "p99_duration_seconds": float(session_summary[7]),
        },
        "sensitivity": sensitivity,
        "marts": [
            {
                "table_name": f"marts.{name}",
                "row_count": count,
                "parquet_path": str((MARTS_DIR / f"{name}.parquet").relative_to(ROOT)),
            }
            for name, count in sorted(mart_counts.items())
        ],
        "core_session_objects": [
            {
                "table_name": "core.event_session_map",
                "grain": "one event assigned to one session under one rule version",
                "primary_key": ["event_id", "session_rule_version"],
                "row_count": validation["event_session_map_rows"],
                "parquet_path": "data/derived/core/event_session_map.parquet",
            },
            {
                "table_name": "core.fct_session",
                "grain": "one reconstructed user session under one rule version",
                "primary_key": ["session_id"],
                "row_count": validation["session_rows"],
                "parquet_path": "data/derived/core/fct_session.parquet",
            },
        ],
    }
    (ARTIFACTS_DIR / "analytics_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    for directory in (CORE_DIR, MARTS_DIR, ARTIFACTS_DIR, DOCS_DIR):
        directory.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    try:
        configure_duckdb(con, ROOT / "data" / "derived" / "duckdb_tmp" / "analytics")
        for schema in ("core", "marts", "meta"):
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        required = [
            "core.fct_event",
            "core.dim_date",
            "core.dim_user_location",
            "core.dim_hotel_market",
            "core.dim_search_params",
        ]
        missing = [
            relation for relation in required
            if scalar(con, f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema || '.' || table_name = {sql_literal(relation)}") == 0
        ]
        if missing:
            raise RuntimeError(f"CORE objects are missing: {', '.join(missing)}")

        con.execute("""
            CREATE OR REPLACE TEMP VIEW train_event_enriched AS
            SELECT
                e.*,
                d.full_date AS event_date,
                d.year_month,
                ul.user_country,
                ul.user_region,
                ul.user_city,
                hm.hotel_country,
                hm.hotel_continent,
                sp.adults_cnt,
                sp.children_cnt,
                sp.room_cnt
            FROM core.fct_event e
            LEFT JOIN core.dim_date d ON d.date_key = e.event_date_key
            LEFT JOIN core.dim_user_location ul ON ul.user_location_id = e.user_location_id
            LEFT JOIN core.dim_hotel_market hm ON hm.hotel_market_id = e.hotel_market_id
            LEFT JOIN core.dim_search_params sp ON sp.search_params_id = e.search_params_id
            WHERE e.source_dataset = 'train'
              AND e.user_id IS NOT NULL
              AND e.event_ts IS NOT NULL
              AND e.event_date_key IS NOT NULL
        """)

        session_glob = build_session_fragments(con)
        event_session_map_sql = f"""
        SELECT event_id, session_id, session_rule_version
        FROM read_parquet({sql_literal(session_glob)})
        """
        materialize(con, "core", "event_session_map", event_session_map_sql, CORE_DIR)

        fct_session_sql = f"""
        SELECT
            session_id,
            session_rule_version,
            user_id,
            source_dataset,
            MIN(event_ts) AS session_start_ts,
            MAX(event_ts) AS session_end_ts,
            arg_min(
                event_date_key,
                struct_pack(event_ts := event_ts, event_id := event_id)
            ) AS session_date_key,
            arg_min(
                event_hour_key,
                struct_pack(event_ts := event_ts, event_id := event_id)
            ) AS session_start_hour_key,
            date_diff('second', MIN(event_ts), MAX(event_ts))::BIGINT
                AS session_duration_seconds,
            COUNT(*)::BIGINT AS row_count,
            SUM(COALESCE(cnt, 0))::BIGINT AS weighted_event_count,
            COUNT(DISTINCT destination_id)::BIGINT AS distinct_destination_count,
            COUNT(DISTINCT hotel_market_id)::BIGINT AS distinct_hotel_market_count,
            COUNT(DISTINCT search_params_id)::BIGINT AS distinct_search_params_count,
            BOOL_OR(is_booking = 1) AS has_booking,
            COUNT(*) FILTER (WHERE is_booking = 1)::BIGINT AS booking_row_count,
            MIN(event_ts) FILTER (WHERE is_booking = 1) AS first_booking_ts,
            date_diff('second', MIN(event_ts), MIN(event_ts) FILTER (WHERE is_booking = 1))
                AS time_to_first_booking_seconds,
            SUM(CASE WHEN is_booking = 1 THEN COALESCE(booking_value_proxy, 0) ELSE 0 END)::BIGINT
                AS booking_value_proxy_total,
            COUNT(*) FILTER (WHERE is_booking = 1 AND is_package = 1)::BIGINT
                AS package_booking_count,
            arg_min(
                channel,
                struct_pack(event_ts := event_ts, event_id := event_id)
            ) AS first_channel,
            arg_max(
                channel,
                struct_pack(event_ts := event_ts, event_id := event_id)
            ) AS last_channel,
            arg_min(
                platform_id,
                struct_pack(event_ts := event_ts, event_id := event_id)
            ) AS first_platform_id,
            arg_max(
                platform_id,
                struct_pack(event_ts := event_ts, event_id := event_id)
            ) AS last_platform_id,
            arg_min(
                destination_id,
                struct_pack(event_ts := event_ts, event_id := event_id)
            ) AS first_destination_id,
            arg_max(
                destination_id,
                struct_pack(event_ts := event_ts, event_id := event_id)
            ) AS last_destination_id,
            arg_min(
                is_mobile,
                struct_pack(event_ts := event_ts, event_id := event_id)
            ) AS first_is_mobile,
            arg_max(
                is_mobile,
                struct_pack(event_ts := event_ts, event_id := event_id)
            ) AS last_is_mobile
        FROM read_parquet({sql_literal(session_glob)})
        GROUP BY session_id, session_rule_version, user_id, source_dataset
        """
        if SESSION_SUMMARIES_DIR.exists():
            for path in SESSION_SUMMARIES_DIR.glob("part_*.parquet"):
                path.unlink()
        else:
            SESSION_SUMMARIES_DIR.mkdir(parents=True, exist_ok=True)
        for bucket in range(SESSION_BUCKETS):
            source_fragment = SESSION_FRAGMENTS_DIR / f"part_{bucket:02d}.parquet"
            summary_fragment = SESSION_SUMMARIES_DIR / f"part_{bucket:02d}.parquet"
            summary_sql = fct_session_sql.replace(
                sql_literal(session_glob), output_path(source_fragment)
            )
            con.execute(
                f"COPY ({summary_sql}) TO {output_path(summary_fragment)} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000)"
            )

        summary_glob = str((SESSION_SUMMARIES_DIR / "part_*.parquet").resolve()).replace("\\", "/")
        materialize(
            con,
            "core",
            "fct_session",
            f"SELECT * FROM read_parquet({sql_literal(summary_glob)})",
            CORE_DIR,
        )

        sensitivity = build_sensitivity(con)
        validation = validate(con)
        mart_counts = build_marts(con)
        write_docs_and_manifest(con, sensitivity, mart_counts, validation)

        con.execute(
            "CREATE OR REPLACE VIEW meta.analytics_build_manifest AS "
            f"SELECT * FROM read_json_auto({output_path(ARTIFACTS_DIR / 'analytics_manifest.json')})"
        )
        print(json.dumps({
            "build_timestamp": BUILD_TS,
            "session_rule_version": SESSION_RULE_VERSION,
            "eligible_train_events": validation["eligible_train_events"],
            "sessions": validation["session_rows"],
            "marts": mart_counts,
            "validation_pass": validation["pass"],
        }, ensure_ascii=False, indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()
