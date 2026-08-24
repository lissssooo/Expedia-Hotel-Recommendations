"""Materialize the Expedia STAGING and CORE layers.

The script is intentionally SQL-first.  It registers the immutable test and
destination Parquet files as RAW views, reads the canonical train Parquet
directly, materializes derived Parquet, and registers derived views in
analytics.duckdb.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

try:
    from tools.duckdb_runtime import configure_duckdb
except ModuleNotFoundError:  # Direct entrypoint: python3 tools/build_core.py
    from duckdb_runtime import configure_duckdb


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "analytics.duckdb"
DERIVED = ROOT / "data" / "derived"
STAGING_DIR = DERIVED / "staging"
CORE_DIR = DERIVED / "core"
ARTIFACTS_DIR = ROOT / "artifacts"
DOCS_DIR = ROOT / "docs"
BUILD_TS = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
MIN_SUPPORT = 5
HOLDOUT_MODULUS = 10
PROJECT_DATE_START = "2013-01-01"
PROJECT_DATE_END = "2016-12-31"
USED_DISTANCE_LEVELS = (
    "city_destination",
    "city_market",
    "region_destination",
    "region_market",
)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def output_path(path: Path) -> str:
    return sql_literal(str(path.resolve()).replace("\\", "/"))


def project_date_is_valid_sql(expression: str) -> str:
    """Return the canonical inclusive project-date predicate for a SQL expression."""
    return (
        f"COALESCE(CAST({expression} AS DATE) BETWEEN "
        f"DATE {sql_literal(PROJECT_DATE_START)} AND DATE {sql_literal(PROJECT_DATE_END)}, FALSE)"
    )


def project_date_is_outside_sql(expression: str) -> str:
    """Flag a present date outside the project range; NULL remains a missing-date issue."""
    return (
        f"({expression} IS NOT NULL AND "
        f"NOT ({project_date_is_valid_sql(expression)}))"
    )


def materialize(con: duckdb.DuckDBPyConnection, layer: str, name: str, query: str, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.parquet"
    if path.exists():
        path.unlink()
    con.execute(
        f"COPY ({query}) TO {output_path(path)} "
        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW {layer}.{name} AS "
        f"SELECT * FROM read_parquet({output_path(path)})"
    )
    return path


def scalar(con: duckdb.DuckDBPyConnection, query: str, column: str):
    return con.execute(query).fetchone()[0]


def table_rows(con: duckdb.DuckDBPyConnection, relation: str) -> int:
    return int(scalar(con, f"SELECT COUNT(*) FROM {relation}", "count"))


def ensure_raw_prerequisites(con: duckdb.DuckDBPyConnection) -> None:
    """Register the source-aligned RAW views required by the CORE build."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    raw_sources = {
        "test": ROOT / "data" / "parquet" / "test.parquet",
        "destinations": ROOT / "data" / "parquet" / "destinations.parquet",
    }
    missing = [path for path in raw_sources.values() if not path.exists()]
    if missing:
        missing_paths = ", ".join(str(path) for path in missing)
        raise RuntimeError(f"Required RAW Parquet source is missing: {missing_paths}")

    for name, path in raw_sources.items():
        con.execute(
            f"CREATE OR REPLACE VIEW raw.{name} AS "
            f"SELECT * FROM read_parquet({output_path(path)})"
        )


def main() -> None:
    for directory in (STAGING_DIR, CORE_DIR, ARTIFACTS_DIR):
        directory.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    try:
        configure_duckdb(con, DERIVED / "duckdb_tmp" / "core")
        ensure_raw_prerequisites(con)
        con.execute("CREATE SCHEMA IF NOT EXISTS staging")
        con.execute("CREATE SCHEMA IF NOT EXISTS core")
        con.execute("CREATE SCHEMA IF NOT EXISTS meta")

        full_train_path = ROOT / "data" / "parquet" / "train_full.parquet"
        if not full_train_path.exists():
            raise RuntimeError(
                "Canonical train source is missing: "
                f"{full_train_path}. Rebuild it from the immutable source parts "
                "before running the CORE build."
            )
        train_source = f"read_parquet({output_path(full_train_path)})"
        train_source_label = "data/parquet/train_full.parquet"

        raw_check = con.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM """ + train_source + """) AS train_rows,
                (SELECT COUNT(*) FROM raw.test) AS test_rows,
                (SELECT COUNT(*) FROM raw.destinations) AS destination_rows
            """
        ).fetchone()
        if raw_check is None or raw_check[0] == 0 or raw_check[1] == 0:
            raise RuntimeError("RAW train/test views are missing or empty")

        staging_interaction_sql = f"""
        WITH source_rows AS (
            SELECT
                'train'::VARCHAR AS source_dataset,
                {sql_literal(train_source_label)}::VARCHAR AS source_file,
                row_number() OVER (
                    ORDER BY date_time, CAST(srch_ci AS VARCHAR), CAST(srch_co AS VARCHAR),
                             site_name, posa_continent, user_location_country,
                             user_location_region, user_location_city,
                             orig_destination_distance NULLS FIRST, user_id,
                             is_mobile, is_package, channel, srch_adults_cnt,
                             srch_children_cnt, srch_rm_cnt, srch_destination_id,
                             srch_destination_type_id, is_booking, cnt,
                             hotel_continent, hotel_country, hotel_market,
                             hotel_cluster
                )::BIGINT AS source_row_id,
                NULL::BIGINT AS source_id,
                date_time::TIMESTAMP AS date_time,
                CAST(srch_ci AS VARCHAR) AS raw_srch_ci,
                CAST(srch_co AS VARCHAR) AS raw_srch_co,
                site_name::BIGINT AS site_name,
                posa_continent::BIGINT AS posa_continent,
                user_location_country::BIGINT AS user_location_country,
                user_location_region::BIGINT AS user_location_region,
                user_location_city::BIGINT AS user_location_city,
                orig_destination_distance::DOUBLE AS orig_destination_distance,
                user_id::BIGINT AS user_id,
                is_mobile::BIGINT AS is_mobile,
                is_package::BIGINT AS is_package,
                channel::BIGINT AS channel,
                srch_adults_cnt::BIGINT AS srch_adults_cnt,
                srch_children_cnt::BIGINT AS srch_children_cnt,
                srch_rm_cnt::BIGINT AS srch_rm_cnt,
                srch_destination_id::BIGINT AS srch_destination_id,
                srch_destination_type_id::BIGINT AS srch_destination_type_id,
                is_booking::BIGINT AS is_booking,
                cnt::BIGINT AS cnt,
                hotel_continent::BIGINT AS hotel_continent,
                hotel_country::BIGINT AS hotel_country,
                hotel_market::BIGINT AS hotel_market,
                hotel_cluster::BIGINT AS hotel_cluster
            FROM {train_source}
            UNION ALL
            SELECT
                'test'::VARCHAR,
                {sql_literal('data/parquet/test.parquet')}::VARCHAR,
                id::BIGINT,
                id::BIGINT,
                date_time::TIMESTAMP,
                CAST(srch_ci AS VARCHAR),
                CAST(srch_co AS VARCHAR),
                site_name::BIGINT,
                posa_continent::BIGINT,
                user_location_country::BIGINT,
                user_location_region::BIGINT,
                user_location_city::BIGINT,
                orig_destination_distance::DOUBLE,
                user_id::BIGINT,
                is_mobile::BIGINT,
                is_package::BIGINT,
                channel::BIGINT,
                srch_adults_cnt::BIGINT,
                srch_children_cnt::BIGINT,
                srch_rm_cnt::BIGINT,
                srch_destination_id::BIGINT,
                srch_destination_type_id::BIGINT,
                NULL::BIGINT,
                NULL::BIGINT,
                hotel_continent::BIGINT,
                hotel_country::BIGINT,
                hotel_market::BIGINT,
                NULL::BIGINT
            FROM raw.test
        ),
        normalized AS (
            SELECT
                source_rows.*,
                date_time AS event_ts,
                TRY_CAST(raw_srch_ci AS DATE) AS checkin_date,
                TRY_CAST(raw_srch_co AS DATE) AS checkout_date
            FROM source_rows
        ),
        ranked AS (
            SELECT
                normalized.*,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        source_dataset, date_time, raw_srch_ci, raw_srch_co,
                        site_name, posa_continent, user_location_country,
                        user_location_region, user_location_city,
                        orig_destination_distance, user_id, is_mobile,
                        is_package, channel, srch_adults_cnt,
                        srch_children_cnt, srch_rm_cnt, srch_destination_id,
                        srch_destination_type_id, is_booking, cnt,
                        hotel_continent, hotel_country, hotel_market,
                        hotel_cluster
                    ORDER BY source_row_id
                ) AS duplicate_rank,
                COUNT(*) OVER (
                    PARTITION BY
                        source_dataset, date_time, raw_srch_ci, raw_srch_co,
                        site_name, posa_continent, user_location_country,
                        user_location_region, user_location_city,
                        orig_destination_distance, user_id, is_mobile,
                        is_package, channel, srch_adults_cnt,
                        srch_children_cnt, srch_rm_cnt, srch_destination_id,
                        srch_destination_type_id, is_booking, cnt,
                        hotel_continent, hotel_country, hotel_market,
                        hotel_cluster
                ) AS duplicate_group_size
            FROM normalized
        ),
        flagged AS (
            SELECT
                ranked.*,
                orig_destination_distance IS NULL AS distance_is_missing,
                checkin_date IS NULL AS q_missing_checkin,
                checkout_date IS NULL AS q_missing_checkout,
                COALESCE(checkin_date < CAST(event_ts AS DATE), FALSE)
                    AS q_checkin_before_event,
                COALESCE(checkout_date < checkin_date, FALSE)
                    AS q_checkout_before_checkin,
                COALESCE(checkout_date = checkin_date, FALSE)
                    AS q_same_day_stay,
                COALESCE(srch_adults_cnt = 0, FALSE) AS q_zero_adults,
                COALESCE(srch_rm_cnt = 0, FALSE) AS q_zero_rooms,
                COALESCE(srch_adults_cnt + srch_children_cnt = 0, FALSE)
                    AS q_zero_travelers,
                COALESCE(
                    {project_date_is_outside_sql('event_ts')}
                    OR {project_date_is_outside_sql('checkin_date')}
                    OR {project_date_is_outside_sql('checkout_date')},
                    FALSE
                ) AS q_extreme_future_date,
                duplicate_group_size > 1 AS q_exact_duplicate,
                {sql_literal(BUILD_TS)}::TIMESTAMP AS loaded_at
            FROM ranked
        )
        SELECT
            *,
            CAST(
                distance_is_missing::INTEGER
                + q_missing_checkin::INTEGER
                + q_missing_checkout::INTEGER
                + q_checkin_before_event::INTEGER
                + q_checkout_before_checkin::INTEGER
                + q_same_day_stay::INTEGER
                + q_zero_adults::INTEGER
                + q_zero_rooms::INTEGER
                + q_zero_travelers::INTEGER
                + q_extreme_future_date::INTEGER
                + q_exact_duplicate::INTEGER
                AS BIGINT
            ) AS quality_issue_count
        FROM flagged
        """
        staging_interaction_path = materialize(
            con, "staging", "interaction", staging_interaction_sql, STAGING_DIR
        )

        staging_destinations_sql = f"""
        SELECT
            srch_destination_id::BIGINT AS destination_id,
            {sql_literal('destinations')}::VARCHAR AS source_dataset,
            {sql_literal('data/parquet/destinations.parquet')}::VARCHAR AS source_file,
            row_number() OVER ()::BIGINT AS source_row_id,
            {sql_literal(BUILD_TS)}::TIMESTAMP AS loaded_at,
            * EXCLUDE (srch_destination_id)
        FROM raw.destinations
        """
        staging_destinations_path = materialize(
            con, "staging", "destinations", staging_destinations_sql, STAGING_DIR
        )

        staging_row_count = table_rows(con, "staging.interaction")
        staging_duplicate_count = int(
            scalar(con, "SELECT COUNT(*) FROM staging.interaction WHERE q_exact_duplicate", "count")
        )

        mapping_sql = """
        WITH x AS (
            SELECT user_id, user_location_country, user_location_region,
                   user_location_city, site_name, posa_continent,
                   hotel_market, hotel_country, hotel_continent,
                   srch_destination_id, srch_destination_type_id
            FROM staging.interaction
        )
        SELECT
            (SELECT COUNT(*) FROM (
                SELECT user_id FROM x GROUP BY user_id
                HAVING COUNT(DISTINCT (user_location_country, user_location_region,
                                       user_location_city)) > 1
            )) AS user_location_violations,
            (SELECT COUNT(*) FROM (
                SELECT site_name FROM x GROUP BY site_name
                HAVING COUNT(DISTINCT posa_continent) > 1
            )) AS site_platform_violations,
            (SELECT COUNT(*) FROM (
                SELECT hotel_market FROM x GROUP BY hotel_market
                HAVING COUNT(DISTINCT (hotel_country, hotel_continent)) > 1
            )) AS hotel_market_violations,
            (SELECT COUNT(*) FROM (
                SELECT srch_destination_id FROM x GROUP BY srch_destination_id
                HAVING COUNT(DISTINCT srch_destination_type_id) > 1
            )) AS destination_type_violations
        """
        mapping_row = con.execute(mapping_sql).fetchone()
        user_location_violations = int(mapping_row[0])
        site_platform_violations = int(mapping_row[1])
        hotel_market_violations = int(mapping_row[2])
        destination_type_violations = int(mapping_row[3])
        user_location_unstable = user_location_violations > 0

        core_base_sql = """
        SELECT
            *,
            srch_adults_cnt AS adults_cnt,
            srch_children_cnt AS children_cnt,
            srch_rm_cnt AS room_cnt,
            CASE
                WHEN valid_for_lead_time THEN DATE_DIFF('day', CAST(event_ts AS DATE), checkin_date)
                ELSE NULL
            END::BIGINT AS lead_days,
            CASE
                WHEN valid_for_stay_length THEN DATE_DIFF('day', checkin_date, checkout_date)
                ELSE NULL
            END::BIGINT AS stay_nights,
            CASE
                WHEN srch_adults_cnt IS NOT NULL AND srch_children_cnt IS NOT NULL
                    THEN srch_adults_cnt + srch_children_cnt
                ELSE NULL
            END::BIGINT AS party_size,
            CASE
                WHEN srch_children_cnt IS NOT NULL THEN srch_children_cnt > 0
                ELSE NULL
            END AS has_children,
            CASE
                WHEN is_booking IS NULL THEN NULL
                WHEN is_booking = 0 THEN 0
                WHEN is_booking = 1 AND is_package = 1 THEN 2
                WHEN is_booking = 1 THEN 1
                ELSE NULL
            END::BIGINT AS booking_value_proxy
        FROM (
            SELECT
                interaction.*,
                (
                    event_ts IS NOT NULL
                    AND checkin_date IS NOT NULL
                    AND NOT q_checkin_before_event
                    AND NOT q_extreme_future_date
                ) AS valid_for_lead_time,
                (
                    checkin_date IS NOT NULL
                    AND checkout_date IS NOT NULL
                    AND NOT q_checkout_before_checkin
                    AND NOT q_same_day_stay
                    AND NOT q_extreme_future_date
                ) AS valid_for_stay_length,
                (
                    srch_adults_cnt IS NOT NULL
                    AND srch_children_cnt IS NOT NULL
                    AND srch_rm_cnt IS NOT NULL
                    AND srch_adults_cnt > 0
                    AND srch_rm_cnt > 0
                    AND srch_adults_cnt + srch_children_cnt > 0
                ) AS valid_for_party_metrics
            FROM staging.interaction AS interaction
            WHERE duplicate_rank = 1
        )
        """
        con.execute(f"CREATE OR REPLACE TEMP VIEW core_base AS {core_base_sql}")
        core_base_count = table_rows(con, "core_base")

        dim_date_sql = f"""
        WITH candidates AS (
            SELECT CAST(event_ts AS DATE) AS full_date
            FROM core_base
            WHERE {project_date_is_valid_sql('event_ts')}
            UNION
            SELECT checkin_date FROM core_base
            WHERE {project_date_is_valid_sql('checkin_date')}
            UNION
            SELECT checkout_date FROM core_base
            WHERE {project_date_is_valid_sql('checkout_date')}
        ), bounds AS (
            SELECT MIN(full_date) AS min_date, MAX(full_date) AS max_date FROM candidates
        ), calendar AS (
            SELECT CAST(gs AS DATE) AS full_date
            FROM bounds, generate_series(min_date, max_date, INTERVAL 1 DAY) AS t(gs)
        )
        SELECT
            (EXTRACT(YEAR FROM full_date)::BIGINT * 10000
             + EXTRACT(MONTH FROM full_date)::BIGINT * 100
             + EXTRACT(DAY FROM full_date)::BIGINT) AS date_key,
            full_date,
            EXTRACT(YEAR FROM full_date)::INTEGER AS year,
            EXTRACT(QUARTER FROM full_date)::INTEGER AS quarter,
            (EXTRACT(YEAR FROM full_date)::VARCHAR || '-Q'
             || EXTRACT(QUARTER FROM full_date)::VARCHAR) AS year_quarter,
            EXTRACT(MONTH FROM full_date)::INTEGER AS month,
            strftime(full_date, '%B') AS month_name,
            strftime(full_date, '%Y-%m') AS year_month,
            EXTRACT(WEEK FROM full_date)::INTEGER AS iso_week,
            EXTRACT(DAY FROM full_date)::INTEGER AS day_of_month,
            EXTRACT(DOY FROM full_date)::INTEGER AS day_of_year,
            EXTRACT(ISODOW FROM full_date)::INTEGER AS day_of_week,
            strftime(full_date, '%A') AS day_name,
            EXTRACT(ISODOW FROM full_date)::INTEGER IN (6, 7) AS is_weekend,
            CASE
                WHEN EXTRACT(MONTH FROM full_date) IN (12, 1, 2) THEN 'winter'
                WHEN EXTRACT(MONTH FROM full_date) IN (3, 4, 5) THEN 'spring'
                WHEN EXTRACT(MONTH FROM full_date) IN (6, 7, 8) THEN 'summer'
                ELSE 'autumn'
            END AS season
        FROM calendar
        """
        materialize(con, "core", "dim_date", dim_date_sql, CORE_DIR)
        dim_hour_sql = """
        SELECT
            hour::BIGINT AS hour_key,
            hour::INTEGER AS hour,
            CASE
                WHEN hour BETWEEN 0 AND 5 THEN 'night'
                WHEN hour BETWEEN 6 AND 11 THEN 'morning'
                WHEN hour BETWEEN 12 AND 17 THEN 'afternoon'
                ELSE 'evening'
            END AS daypart
        FROM range(24) AS t(hour)
        """
        materialize(con, "core", "dim_hour", dim_hour_sql, CORE_DIR)

        dim_user_sql = """
        SELECT DISTINCT user_id
        FROM core_base
        WHERE user_id IS NOT NULL
        """
        materialize(con, "core", "dim_user", dim_user_sql, CORE_DIR)

        if user_location_unstable:
            dim_user_location_sql = """
            WITH locations AS (
                SELECT DISTINCT
                    user_location_country AS user_country,
                    user_location_region AS user_region,
                    user_location_city AS user_city
                FROM core_base
            )
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY user_country NULLS FIRST, user_region NULLS FIRST,
                             user_city NULLS FIRST
                )::BIGINT AS user_location_id,
                user_country,
                user_region,
                user_city
            FROM locations
            """
            materialize(con, "core", "dim_user_location", dim_user_location_sql, CORE_DIR)

        dim_platform_sql = """
        WITH platforms AS (
            SELECT DISTINCT site_name, posa_continent FROM core_base
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY site_name NULLS FIRST, posa_continent NULLS FIRST)::BIGINT
                AS platform_id,
            site_name,
            posa_continent
        FROM platforms
        """
        materialize(con, "core", "dim_platform", dim_platform_sql, CORE_DIR)

        dim_destination_type_sql = """
        SELECT DISTINCT srch_destination_type_id AS destination_type_id
        FROM core_base
        WHERE srch_destination_type_id IS NOT NULL
        """
        materialize(con, "core", "dim_destination_type", dim_destination_type_sql, CORE_DIR)

        dim_destination_sql = """
        WITH event_destinations AS (
            SELECT
                srch_destination_id AS destination_id,
                MAX(srch_destination_type_id) AS destination_type_id
            FROM core_base
            WHERE srch_destination_id IS NOT NULL
            GROUP BY srch_destination_id
        ), destination_features AS (
            SELECT * FROM staging.destinations
        )
        SELECT
            COALESCE(e.destination_id, d.destination_id)::BIGINT AS destination_id,
            e.destination_type_id,
            d.* EXCLUDE (destination_id, source_dataset, source_file, source_row_id, loaded_at)
        FROM event_destinations e
        FULL OUTER JOIN destination_features d USING (destination_id)
        """
        materialize(con, "core", "dim_destination", dim_destination_sql, CORE_DIR)

        dim_hotel_market_sql = """
        WITH markets AS (
            SELECT DISTINCT hotel_continent, hotel_country, hotel_market
            FROM core_base
            WHERE hotel_market IS NOT NULL
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY hotel_market, hotel_country NULLS FIRST, hotel_continent NULLS FIRST
            )::BIGINT AS hotel_market_id,
            hotel_continent,
            hotel_country,
            hotel_market
        FROM markets
        """
        materialize(con, "core", "dim_hotel_market", dim_hotel_market_sql, CORE_DIR)

        dim_hotel_cluster_sql = """
        SELECT DISTINCT hotel_cluster AS hotel_cluster_id
        FROM core_base
        WHERE hotel_cluster IS NOT NULL
        """
        materialize(con, "core", "dim_hotel_cluster", dim_hotel_cluster_sql, CORE_DIR)

        dim_search_params_sql = """
        WITH params AS (
            SELECT DISTINCT adults_cnt, children_cnt, room_cnt,
                            stay_nights, party_size, has_children
            FROM core_base
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY adults_cnt NULLS FIRST, children_cnt NULLS FIRST,
                         room_cnt NULLS FIRST, stay_nights NULLS FIRST,
                         party_size NULLS FIRST, has_children NULLS FIRST
            )::BIGINT AS search_params_id,
            adults_cnt,
            children_cnt,
            room_cnt,
            stay_nights,
            party_size,
            has_children
        FROM params
        """
        materialize(con, "core", "dim_search_params", dim_search_params_sql, CORE_DIR)

        distance_validation_sql = f"""
        WITH observed AS (
            SELECT
                source_dataset, source_row_id, orig_destination_distance AS distance_raw,
                user_location_city AS origin_city,
                user_location_region AS origin_region,
                user_location_country AS origin_country,
                srch_destination_id AS destination_id,
                hotel_market,
                hotel_country,
                hash(source_dataset, source_row_id) % {HOLDOUT_MODULUS} = 0 AS is_holdout
            FROM core_base
            WHERE orig_destination_distance IS NOT NULL
        ),
        stats AS (
            SELECT 'city_destination' AS imputation_level, origin_city, NULL::BIGINT AS origin_region,
                   NULL::BIGINT AS origin_country, destination_id, NULL::BIGINT AS hotel_market,
                   NULL::BIGINT AS hotel_country, median(distance_raw) AS median_distance,
                   COUNT(*)::BIGINT AS observations
            FROM observed WHERE NOT is_holdout AND origin_city IS NOT NULL AND destination_id IS NOT NULL
            GROUP BY origin_city, destination_id
            UNION ALL
            SELECT 'city_market', origin_city, NULL, NULL, NULL, hotel_market, NULL,
                   median(distance_raw), COUNT(*)
            FROM observed WHERE NOT is_holdout AND origin_city IS NOT NULL AND hotel_market IS NOT NULL
            GROUP BY origin_city, hotel_market
            UNION ALL
            SELECT 'region_destination', NULL, origin_region, NULL, destination_id, NULL, NULL,
                   median(distance_raw), COUNT(*)
            FROM observed WHERE NOT is_holdout AND origin_region IS NOT NULL AND destination_id IS NOT NULL
            GROUP BY origin_region, destination_id
            UNION ALL
            SELECT 'region_market', NULL, origin_region, NULL, NULL, hotel_market, NULL,
                   median(distance_raw), COUNT(*)
            FROM observed WHERE NOT is_holdout AND origin_region IS NOT NULL AND hotel_market IS NOT NULL
            GROUP BY origin_region, hotel_market
            UNION ALL
            SELECT 'country_destination', NULL, NULL, origin_country, destination_id, NULL, NULL,
                   median(distance_raw), COUNT(*)
            FROM observed WHERE NOT is_holdout AND origin_country IS NOT NULL AND destination_id IS NOT NULL
            GROUP BY origin_country, destination_id
            UNION ALL
            SELECT 'country_market', NULL, NULL, origin_country, NULL, hotel_market, NULL,
                   median(distance_raw), COUNT(*)
            FROM observed WHERE NOT is_holdout AND origin_country IS NOT NULL AND hotel_market IS NOT NULL
            GROUP BY origin_country, hotel_market
            UNION ALL
            SELECT 'country_hotel_country', NULL, NULL, origin_country, NULL, NULL, hotel_country,
                   median(distance_raw), COUNT(*)
            FROM observed WHERE NOT is_holdout AND origin_country IS NOT NULL AND hotel_country IS NOT NULL
            GROUP BY origin_country, hotel_country
        ),
        candidates AS (
            SELECT * FROM (VALUES (1), (3), (5), (10), (20), (50), (100)) AS t(min_support)
        ),
        predictions AS (
            SELECT o.*, 'city_destination' AS imputation_level, s.median_distance, s.observations
            FROM observed o LEFT JOIN stats s ON s.imputation_level = 'city_destination'
                AND s.origin_city IS NOT DISTINCT FROM o.origin_city
                AND s.destination_id IS NOT DISTINCT FROM o.destination_id
            WHERE o.is_holdout
            UNION ALL
            SELECT o.*, 'city_market', s.median_distance, s.observations
            FROM observed o LEFT JOIN stats s ON s.imputation_level = 'city_market'
                AND s.origin_city IS NOT DISTINCT FROM o.origin_city
                AND s.hotel_market IS NOT DISTINCT FROM o.hotel_market
            WHERE o.is_holdout
            UNION ALL
            SELECT o.*, 'region_destination', s.median_distance, s.observations
            FROM observed o LEFT JOIN stats s ON s.imputation_level = 'region_destination'
                AND s.origin_region IS NOT DISTINCT FROM o.origin_region
                AND s.destination_id IS NOT DISTINCT FROM o.destination_id
            WHERE o.is_holdout
            UNION ALL
            SELECT o.*, 'region_market', s.median_distance, s.observations
            FROM observed o LEFT JOIN stats s ON s.imputation_level = 'region_market'
                AND s.origin_region IS NOT DISTINCT FROM o.origin_region
                AND s.hotel_market IS NOT DISTINCT FROM o.hotel_market
            WHERE o.is_holdout
            UNION ALL
            SELECT o.*, 'country_destination', s.median_distance, s.observations
            FROM observed o LEFT JOIN stats s ON s.imputation_level = 'country_destination'
                AND s.origin_country IS NOT DISTINCT FROM o.origin_country
                AND s.destination_id IS NOT DISTINCT FROM o.destination_id
            WHERE o.is_holdout
            UNION ALL
            SELECT o.*, 'country_market', s.median_distance, s.observations
            FROM observed o LEFT JOIN stats s ON s.imputation_level = 'country_market'
                AND s.origin_country IS NOT DISTINCT FROM o.origin_country
                AND s.hotel_market IS NOT DISTINCT FROM o.hotel_market
            WHERE o.is_holdout
            UNION ALL
            SELECT o.*, 'country_hotel_country', s.median_distance, s.observations
            FROM observed o LEFT JOIN stats s ON s.imputation_level = 'country_hotel_country'
                AND s.origin_country IS NOT DISTINCT FROM o.origin_country
                AND s.hotel_country IS NOT DISTINCT FROM o.hotel_country
            WHERE o.is_holdout
        )
        SELECT
            imputation_level,
            c.min_support,
            COUNT(*)::BIGINT AS holdout_rows,
            COUNT(*) FILTER (WHERE observations >= c.min_support)::BIGINT AS covered_rows,
            100.0 * COUNT(*) FILTER (WHERE observations >= c.min_support) / NULLIF(COUNT(*), 0)
                AS coverage_pct,
            AVG(ABS(distance_raw - median_distance))
                FILTER (WHERE observations >= c.min_support) AS mae,
            MEDIAN(ABS(distance_raw - median_distance))
                FILTER (WHERE observations >= c.min_support) AS median_absolute_error,
            QUANTILE_CONT(ABS(distance_raw - median_distance), 0.9)
                FILTER (WHERE observations >= c.min_support) AS p90_absolute_error,
            AVG(observations) FILTER (WHERE observations >= c.min_support) AS average_support
        FROM predictions p CROSS JOIN candidates c
        GROUP BY imputation_level, c.min_support
        """
        validation_df = con.execute(distance_validation_sql).fetchdf()
        validation_df.to_parquet(CORE_DIR / "distance_validation.parquet", index=False)
        con.register("distance_validation_df", validation_df)

        distance_stats_sql = f"""
        WITH observed AS (
            SELECT
                user_location_city AS origin_city,
                user_location_region AS origin_region,
                user_location_country AS origin_country,
                srch_destination_id AS destination_id,
                hotel_market,
                hotel_country,
                orig_destination_distance AS distance_raw
            FROM core_base
            WHERE orig_destination_distance IS NOT NULL
        ), stats AS (
            SELECT 'city_destination' AS imputation_level, origin_city, NULL::BIGINT AS origin_region,
                   NULL::BIGINT AS origin_country, destination_id, NULL::BIGINT AS hotel_market,
                   NULL::BIGINT AS hotel_country, median(distance_raw) AS median_distance,
                   COUNT(*)::BIGINT AS observations
            FROM observed WHERE origin_city IS NOT NULL AND destination_id IS NOT NULL
            GROUP BY origin_city, destination_id
            UNION ALL
            SELECT 'city_market', origin_city, NULL, NULL, NULL, hotel_market, NULL,
                   median(distance_raw), COUNT(*) FROM observed
            WHERE origin_city IS NOT NULL AND hotel_market IS NOT NULL GROUP BY origin_city, hotel_market
            UNION ALL
            SELECT 'region_destination', NULL, origin_region, NULL, destination_id, NULL, NULL,
                   median(distance_raw), COUNT(*) FROM observed
            WHERE origin_region IS NOT NULL AND destination_id IS NOT NULL GROUP BY origin_region, destination_id
            UNION ALL
            SELECT 'region_market', NULL, origin_region, NULL, NULL, hotel_market, NULL,
                   median(distance_raw), COUNT(*) FROM observed
            WHERE origin_region IS NOT NULL AND hotel_market IS NOT NULL GROUP BY origin_region, hotel_market
            UNION ALL
            SELECT 'country_destination', NULL, NULL, origin_country, destination_id, NULL, NULL,
                   median(distance_raw), COUNT(*) FROM observed
            WHERE origin_country IS NOT NULL AND destination_id IS NOT NULL GROUP BY origin_country, destination_id
            UNION ALL
            SELECT 'country_market', NULL, NULL, origin_country, NULL, hotel_market, NULL,
                   median(distance_raw), COUNT(*) FROM observed
            WHERE origin_country IS NOT NULL AND hotel_market IS NOT NULL GROUP BY origin_country, hotel_market
            UNION ALL
            SELECT 'country_hotel_country', NULL, NULL, origin_country, NULL, NULL, hotel_country,
                   median(distance_raw), COUNT(*) FROM observed
            WHERE origin_country IS NOT NULL AND hotel_country IS NOT NULL GROUP BY origin_country, hotel_country
        )
        SELECT
            s.imputation_level,
            s.origin_city,
            s.origin_region,
            s.origin_country,
            s.destination_id,
            s.hotel_market,
            s.hotel_country,
            s.median_distance,
            s.observations,
            {MIN_SUPPORT}::BIGINT AS minimum_support,
            v.coverage_pct AS validation_coverage_pct,
            v.mae AS validation_mae,
            v.median_absolute_error AS validation_median_absolute_error,
            v.p90_absolute_error AS validation_p90_absolute_error
        FROM stats s
        LEFT JOIN distance_validation_df v
          ON v.imputation_level = s.imputation_level
         AND v.min_support = {MIN_SUPPORT}
        """
        materialize(con, "core", "ref_distance_stats", distance_stats_sql, CORE_DIR)

        fct_event_sql = f"""
        WITH numbered AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY source_dataset, source_row_id)::BIGINT AS event_id,
                core_base.*
            FROM core_base
        ), joined AS (
            SELECT
                n.*,
                du.user_id AS dim_user_id,
                {('ul.user_location_id' if user_location_unstable else 'NULL::BIGINT')} AS dim_user_location_id,
                p.platform_id,
                d.destination_id AS dim_destination_id,
                hm.hotel_market_id,
                hc.hotel_cluster_id,
                sp.search_params_id,
                de.date_key AS event_date_key,
                dh.hour_key AS event_hour_key,
                dci.date_key AS checkin_date_key,
                dco.date_key AS checkout_date_key,
                ds1.median_distance AS city_destination_distance,
                ds1.observations AS city_destination_support,
                ds1.validation_mae AS city_destination_mae,
                ds2.median_distance AS city_market_distance,
                ds2.observations AS city_market_support,
                ds2.validation_mae AS city_market_mae,
                ds3.median_distance AS region_destination_distance,
                ds3.observations AS region_destination_support,
                ds3.validation_mae AS region_destination_mae,
                ds4.median_distance AS region_market_distance,
                ds4.observations AS region_market_support,
                ds4.validation_mae AS region_market_mae,
                ds5.median_distance AS country_destination_distance,
                ds5.observations AS country_destination_support,
                ds5.validation_mae AS country_destination_mae,
                ds6.median_distance AS country_market_distance,
                ds6.observations AS country_market_support,
                ds6.validation_mae AS country_market_mae,
                ds7.median_distance AS country_hotel_country_distance,
                ds7.observations AS country_hotel_country_support,
                ds7.validation_mae AS country_hotel_country_mae
            FROM numbered n
            LEFT JOIN core.dim_user du ON du.user_id = n.user_id
            {('LEFT JOIN core.dim_user_location ul ON ul.user_country IS NOT DISTINCT FROM n.user_location_country AND ul.user_region IS NOT DISTINCT FROM n.user_location_region AND ul.user_city IS NOT DISTINCT FROM n.user_location_city' if user_location_unstable else '')}
            LEFT JOIN core.dim_platform p
              ON p.site_name IS NOT DISTINCT FROM n.site_name
             AND p.posa_continent IS NOT DISTINCT FROM n.posa_continent
            LEFT JOIN core.dim_destination d ON d.destination_id = n.srch_destination_id
            LEFT JOIN core.dim_hotel_market hm
              ON hm.hotel_market IS NOT DISTINCT FROM n.hotel_market
             AND hm.hotel_country IS NOT DISTINCT FROM n.hotel_country
             AND hm.hotel_continent IS NOT DISTINCT FROM n.hotel_continent
            LEFT JOIN core.dim_hotel_cluster hc ON hc.hotel_cluster_id = n.hotel_cluster
            LEFT JOIN core.dim_search_params sp
              ON sp.adults_cnt IS NOT DISTINCT FROM n.adults_cnt
             AND sp.children_cnt IS NOT DISTINCT FROM n.children_cnt
             AND sp.room_cnt IS NOT DISTINCT FROM n.room_cnt
             AND sp.stay_nights IS NOT DISTINCT FROM n.stay_nights
             AND sp.party_size IS NOT DISTINCT FROM n.party_size
             AND sp.has_children IS NOT DISTINCT FROM n.has_children
            LEFT JOIN core.dim_date de ON de.full_date = CAST(n.event_ts AS DATE)
            LEFT JOIN core.dim_hour dh ON dh.hour = EXTRACT(HOUR FROM n.event_ts)
            LEFT JOIN core.dim_date dci ON dci.full_date = n.checkin_date
            LEFT JOIN core.dim_date dco ON dco.full_date = n.checkout_date
            LEFT JOIN core.ref_distance_stats ds1
              ON ds1.imputation_level = 'city_destination' AND ds1.observations >= {MIN_SUPPORT}
             AND ds1.origin_city IS NOT DISTINCT FROM n.user_location_city
             AND ds1.destination_id IS NOT DISTINCT FROM n.srch_destination_id
            LEFT JOIN core.ref_distance_stats ds2
              ON ds2.imputation_level = 'city_market' AND ds2.observations >= {MIN_SUPPORT}
             AND ds2.origin_city IS NOT DISTINCT FROM n.user_location_city
             AND ds2.hotel_market IS NOT DISTINCT FROM n.hotel_market
            LEFT JOIN core.ref_distance_stats ds3
              ON ds3.imputation_level = 'region_destination' AND ds3.observations >= {MIN_SUPPORT}
             AND ds3.origin_region IS NOT DISTINCT FROM n.user_location_region
             AND ds3.destination_id IS NOT DISTINCT FROM n.srch_destination_id
            LEFT JOIN core.ref_distance_stats ds4
              ON ds4.imputation_level = 'region_market' AND ds4.observations >= {MIN_SUPPORT}
             AND ds4.origin_region IS NOT DISTINCT FROM n.user_location_region
             AND ds4.hotel_market IS NOT DISTINCT FROM n.hotel_market
            LEFT JOIN core.ref_distance_stats ds5
              ON ds5.imputation_level = 'country_destination' AND ds5.observations >= {MIN_SUPPORT}
             AND ds5.origin_country IS NOT DISTINCT FROM n.user_location_country
             AND ds5.destination_id IS NOT DISTINCT FROM n.srch_destination_id
            LEFT JOIN core.ref_distance_stats ds6
              ON ds6.imputation_level = 'country_market' AND ds6.observations >= {MIN_SUPPORT}
             AND ds6.origin_country IS NOT DISTINCT FROM n.user_location_country
             AND ds6.hotel_market IS NOT DISTINCT FROM n.hotel_market
            LEFT JOIN core.ref_distance_stats ds7
              ON ds7.imputation_level = 'country_hotel_country' AND ds7.observations >= {MIN_SUPPORT}
             AND ds7.origin_country IS NOT DISTINCT FROM n.user_location_country
             AND ds7.hotel_country IS NOT DISTINCT FROM n.hotel_country
        )
        SELECT
            event_id,
            source_row_id,
            source_dataset,
            event_ts,
            event_date_key,
            event_hour_key,
            checkin_date_key,
            checkout_date_key,
            dim_user_id AS user_id,
            dim_user_location_id AS user_location_id,
            platform_id,
            dim_destination_id AS destination_id,
            hotel_market_id,
            hotel_cluster_id,
            search_params_id,
            channel,
            is_mobile,
            is_package,
            is_booking,
            cnt,
            lead_days,
            stay_nights,
            party_size,
            has_children,
            booking_value_proxy,
            orig_destination_distance AS distance_raw,
            CASE WHEN orig_destination_distance IS NOT NULL THEN orig_destination_distance
                 ELSE COALESCE(
                     CASE WHEN city_destination_support >= {MIN_SUPPORT} THEN city_destination_distance END,
                     CASE WHEN city_market_support >= {MIN_SUPPORT} THEN city_market_distance END,
                     CASE WHEN region_destination_support >= {MIN_SUPPORT} THEN region_destination_distance END,
                     CASE WHEN region_market_support >= {MIN_SUPPORT} THEN region_market_distance END
                 ) END AS distance_filled,
            orig_destination_distance IS NULL AS distance_was_missing,
            orig_destination_distance IS NULL AND COALESCE(
                CASE WHEN city_destination_support >= {MIN_SUPPORT} THEN city_destination_distance END,
                CASE WHEN city_market_support >= {MIN_SUPPORT} THEN city_market_distance END,
                CASE WHEN region_destination_support >= {MIN_SUPPORT} THEN region_destination_distance END,
                CASE WHEN region_market_support >= {MIN_SUPPORT} THEN region_market_distance END
            ) IS NOT NULL AS distance_is_imputed,
            CASE WHEN orig_destination_distance IS NOT NULL THEN 'observed'
                 WHEN city_destination_support >= {MIN_SUPPORT} THEN 'city_destination'
                 WHEN city_market_support >= {MIN_SUPPORT} THEN 'city_market'
                 WHEN region_destination_support >= {MIN_SUPPORT} THEN 'region_destination'
                 WHEN region_market_support >= {MIN_SUPPORT} THEN 'region_market'
                 ELSE NULL END AS distance_imputation_level,
            CASE WHEN orig_destination_distance IS NULL THEN COALESCE(
                CASE WHEN city_destination_support >= {MIN_SUPPORT} THEN city_destination_support END,
                CASE WHEN city_market_support >= {MIN_SUPPORT} THEN city_market_support END,
                CASE WHEN region_destination_support >= {MIN_SUPPORT} THEN region_destination_support END,
                CASE WHEN region_market_support >= {MIN_SUPPORT} THEN region_market_support END
            ) END AS distance_imputation_support,
            CASE WHEN orig_destination_distance IS NULL THEN COALESCE(
                CASE WHEN city_destination_support >= {MIN_SUPPORT} THEN city_destination_mae END,
                CASE WHEN city_market_support >= {MIN_SUPPORT} THEN city_market_mae END,
                CASE WHEN region_destination_support >= {MIN_SUPPORT} THEN region_destination_mae END,
                CASE WHEN region_market_support >= {MIN_SUPPORT} THEN region_market_mae END
            ) END AS distance_imputation_mae,
            valid_for_lead_time,
            valid_for_stay_length,
            valid_for_party_metrics,
            quality_issue_count
        FROM joined
        """
        fct_event_path = materialize(con, "core", "fct_event", fct_event_sql, CORE_DIR)

        fct_booking_sql = """
        SELECT
            event_id AS booking_id,
            event_id,
            user_id,
            user_location_id,
            platform_id,
            destination_id,
            hotel_market_id,
            hotel_cluster_id,
            event_date_key,
            checkin_date_key,
            checkout_date_key,
            is_package,
            lead_days,
            stay_nights,
            distance_filled,
            booking_value_proxy
        FROM core.fct_event
        WHERE is_booking = 1
        """
        fct_booking_path = materialize(con, "core", "fct_booking", fct_booking_sql, CORE_DIR)

        core_tables = [
            "dim_date", "dim_hour", "dim_user",
            *(["dim_user_location"] if user_location_unstable else []),
            "dim_platform", "dim_destination", "dim_destination_type",
            "dim_hotel_market", "dim_hotel_cluster", "dim_search_params",
            "fct_event", "fct_booking", "ref_distance_stats",
        ]

        pk_checks: dict[str, dict[str, int | bool]] = {}
        pk_columns = {
            "dim_date": "date_key",
            "dim_hour": "hour_key",
            "dim_user": "user_id",
            "dim_user_location": "user_location_id",
            "dim_platform": "platform_id",
            "dim_destination": "destination_id",
            "dim_destination_type": "destination_type_id",
            "dim_hotel_market": "hotel_market_id",
            "dim_hotel_cluster": "hotel_cluster_id",
            "dim_search_params": "search_params_id",
            "fct_event": "event_id",
            "fct_booking": "booking_id",
            "ref_distance_stats": "imputation_level, origin_city, origin_region, origin_country, destination_id, hotel_market, hotel_country",
        }
        for table in core_tables:
            pk = pk_columns[table]
            row_count = table_rows(con, f"core.{table}")
            null_pk = int(scalar(con, f"SELECT COUNT(*) FROM core.{table} WHERE {pk.split(',')[0]} IS NULL", "count"))
            duplicate_pk = int(
                scalar(
                    con,
                    f"SELECT COUNT(*) FROM (SELECT {pk} FROM core.{table} GROUP BY {pk} HAVING COUNT(*) > 1)",
                    "count",
                )
            )
            pk_checks[table] = {
                "row_count": row_count,
                "null_pk": null_pk,
                "duplicate_pk_groups": duplicate_pk,
                "pass": null_pk == 0 and duplicate_pk == 0,
            }

        fk_checks = {
            "event_user": scalar(con, "SELECT COUNT(*) FROM core.fct_event e LEFT JOIN core.dim_user d ON e.user_id=d.user_id WHERE e.user_id IS NOT NULL AND d.user_id IS NULL", "count"),
            "event_user_location": scalar(con, "SELECT COUNT(*) FROM core.fct_event e LEFT JOIN core.dim_user_location d ON e.user_location_id=d.user_location_id WHERE e.user_location_id IS NOT NULL AND d.user_location_id IS NULL", "count") if user_location_unstable else 0,
            "event_platform": scalar(con, "SELECT COUNT(*) FROM core.fct_event e LEFT JOIN core.dim_platform d ON e.platform_id=d.platform_id WHERE e.platform_id IS NOT NULL AND d.platform_id IS NULL", "count"),
            "event_destination": scalar(con, "SELECT COUNT(*) FROM core.fct_event e LEFT JOIN core.dim_destination d ON e.destination_id=d.destination_id WHERE e.destination_id IS NOT NULL AND d.destination_id IS NULL", "count"),
            "event_hotel_market": scalar(con, "SELECT COUNT(*) FROM core.fct_event e LEFT JOIN core.dim_hotel_market d ON e.hotel_market_id=d.hotel_market_id WHERE e.hotel_market_id IS NOT NULL AND d.hotel_market_id IS NULL", "count"),
            "event_hotel_cluster": scalar(con, "SELECT COUNT(*) FROM core.fct_event e LEFT JOIN core.dim_hotel_cluster d ON e.hotel_cluster_id=d.hotel_cluster_id WHERE e.hotel_cluster_id IS NOT NULL AND d.hotel_cluster_id IS NULL", "count"),
            "event_search_params": scalar(con, "SELECT COUNT(*) FROM core.fct_event e LEFT JOIN core.dim_search_params d ON e.search_params_id=d.search_params_id WHERE e.search_params_id IS NOT NULL AND d.search_params_id IS NULL", "count"),
            "event_date": scalar(con, "SELECT COUNT(*) FROM core.fct_event e LEFT JOIN core.dim_date d ON e.event_date_key=d.date_key WHERE e.event_date_key IS NOT NULL AND d.date_key IS NULL", "count"),
            "event_hour": scalar(con, "SELECT COUNT(*) FROM core.fct_event e LEFT JOIN core.dim_hour d ON e.event_hour_key=d.hour_key WHERE e.event_hour_key IS NOT NULL AND d.hour_key IS NULL", "count"),
            "event_checkin_date": scalar(con, "SELECT COUNT(*) FROM core.fct_event e LEFT JOIN core.dim_date d ON e.checkin_date_key=d.date_key WHERE e.checkin_date_key IS NOT NULL AND d.date_key IS NULL", "count"),
            "event_checkout_date": scalar(con, "SELECT COUNT(*) FROM core.fct_event e LEFT JOIN core.dim_date d ON e.checkout_date_key=d.date_key WHERE e.checkout_date_key IS NOT NULL AND d.date_key IS NULL", "count"),
        }
        fk_checks = {key: int(value) for key, value in fk_checks.items()}

        fct_count = table_rows(con, "core.fct_event")
        fanout_check = {
            "core_base_rows": core_base_count,
            "fct_event_rows": fct_count,
            "pass": core_base_count == fct_count,
        }
        distance_summary = con.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE distance_was_missing) AS raw_missing_rows,
                COUNT(*) FILTER (WHERE distance_filled IS NULL) AS final_missing_rows,
                COUNT(*) FILTER (WHERE distance_is_imputed) AS imputed_rows,
                100.0 * COUNT(*) FILTER (WHERE distance_was_missing) / COUNT(*) AS raw_missing_pct,
                100.0 * COUNT(*) FILTER (WHERE distance_filled IS NOT NULL) / COUNT(*) AS final_coverage_pct,
                100.0 * COUNT(*) FILTER (WHERE distance_is_imputed) / COUNT(*) AS imputed_pct
            FROM core.fct_event
            """
        ).fetchone()
        distance_by_level = con.execute(
            """
            SELECT distance_imputation_level, COUNT(*) AS rows
            FROM core.fct_event
            WHERE distance_is_imputed
            GROUP BY distance_imputation_level
            ORDER BY rows DESC
            """
        ).fetchdf()

        validation_records = validation_df.to_dict(orient="records")
        validation_for_support = [
            row for row in validation_records if int(row["min_support"]) == MIN_SUPPORT
        ]
        quality_counts = con.execute(
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE q_missing_checkin) AS q_missing_checkin,
                COUNT(*) FILTER (WHERE q_missing_checkout) AS q_missing_checkout,
                COUNT(*) FILTER (WHERE q_checkin_before_event) AS q_checkin_before_event,
                COUNT(*) FILTER (WHERE q_checkout_before_checkin) AS q_checkout_before_checkin,
                COUNT(*) FILTER (WHERE q_same_day_stay) AS q_same_day_stay,
                COUNT(*) FILTER (WHERE q_zero_adults) AS q_zero_adults,
                COUNT(*) FILTER (WHERE q_zero_rooms) AS q_zero_rooms,
                COUNT(*) FILTER (WHERE q_zero_travelers) AS q_zero_travelers,
                COUNT(*) FILTER (WHERE q_extreme_future_date) AS q_extreme_future_date,
                COUNT(*) FILTER (WHERE q_exact_duplicate) AS q_exact_duplicate
            FROM staging.interaction
            """
        ).fetchone()

        core_schema = f"""# CORE schema

Build timestamp: `{BUILD_TS}`
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

`dim_user_location` is present because `{user_location_violations:,}` user IDs have multiple observed location combinations. `hotel_market` is keyed by the actual attribute combination because `{hotel_market_violations:,}` market IDs violate the one-to-one country/continent mapping.

## Fact semantics

`fct_event` is not a click, search, session, or booking journey. `cnt` remains the multiplicity of the aggregated source row. `search_params_id` is only a surrogate for a parameter combination and is never a search/session identifier. `hotel_cluster_id` is a cluster, not a hotel ID.

`booking_value_proxy` is 0 for non-bookings, 1 for hotel-only bookings, and 2 for package bookings; it is not money or revenue. `fct_booking` filters `is_booking = 1` and therefore contains train observations only.

## Quality and validity

STAGING preserves source grain and source values, including NULL distance. It adds date parsing, duplicate metadata, and quality flags. The active project-date range is inclusive from `{PROJECT_DATE_START}` through `{PROJECT_DATE_END}` for event, check-in, and check-out dates. The legacy `q_extreme_future_date` field flags any present date outside that range. CORE keeps the first row of each exact source-payload duplicate group using deterministic `source_row_id` order. Suspicious records are not removed for quality reasons: out-of-range dates receive no `dim_date` key, and derived date metrics remain NULL. `lead_days` and `stay_nights` are populated only under their validity flags; same-day stays are excluded from `valid_for_stay_length` because their business meaning is ambiguous.

## Distance

`distance_raw` is immutable source distance. Missing values are filled only in CORE using median estimators and minimum support `{MIN_SUPPORT}` in this order: city×destination, city×hotel market, region×destination, region×hotel market. The three country-level candidates are measured in the holdout but not applied because their errors are materially broader. Provenance and holdout validation metrics are stored in `fct_event` and `ref_distance_stats`.

## Validation snapshot

- RAW rows: `{raw_check[0] + raw_check[1]:,}` (`train={raw_check[0]:,}`, `test={raw_check[1]:,}`)
- STAGING interaction rows: `{staging_row_count:,}`
- exact-duplicate rows flagged in STAGING: `{staging_duplicate_count:,}`
- CORE event rows: `{core_base_count:,}`
- rows removed by controlled exact deduplication: `{staging_row_count - core_base_count:,}`
- fct_event fan-out check: `{'PASS' if fanout_check['pass'] else 'FAIL'}`
"""
        (DOCS_DIR / "core_schema.md").write_text(core_schema, encoding="utf-8")

        def fmt(value) -> str:
            if value is None:
                return "NULL"
            if isinstance(value, float):
                return f"{value:.3f}"
            return f"{value:,}" if isinstance(value, int) else str(value)

        validation_lines = [
            "| level | support | coverage % | MAE | median AE | p90 AE |",
            "|---|---:|---:|---:|---:|---:|",
        ]
        for row in validation_for_support:
            validation_lines.append(
                f"| {row['imputation_level']} | {int(row['min_support'])} | "
                f"{fmt(row['coverage_pct'])} | {fmt(row['mae'])} | "
                f"{fmt(row['median_absolute_error'])} | {fmt(row['p90_absolute_error'])} |"
            )
        level_lines = ["| level | imputed rows |", "|---|---:|"]
        for row in distance_by_level.to_dict(orient="records"):
            level_lines.append(f"| {row['distance_imputation_level']} | {int(row['rows']):,} |")
        distance_report = f"""# Distance imputation report

Build timestamp: `{BUILD_TS}`
Holdout: deterministic 10% of observed distances (`hash(source_dataset, source_row_id) % 10 = 0`)
Estimator: group median
Selected minimum support: **{MIN_SUPPORT}**

## Coverage

| metric | rows | percent |
|---|---:|---:|
| CORE events | {int(distance_summary[0]):,} | 100.000 |
| missing in source | {int(distance_summary[1]):,} | {fmt(distance_summary[4])} |
| filled after CORE | {int(distance_summary[0] - distance_summary[2]):,} | {fmt(distance_summary[5])} |
| imputed | {int(distance_summary[3]):,} | {fmt(distance_summary[6])} |
| final NULL | {int(distance_summary[2]):,} | {fmt(100.0 * distance_summary[2] / distance_summary[0])} |

## Holdout validation at selected support

{chr(10).join(validation_lines)}

At support 1 and 3, sparse groups have worse error; support 5 is the first tested threshold with a stable error/coverage trade-off. Support 10 and above reduce coverage materially, so support 5 is retained as the reproducible technical threshold. The country-level candidates are not applied: their selected-support p90 errors are materially broader than the city/region levels. No global mean or zero imputation is used.

## Applied hierarchy

{chr(10).join(level_lines) if level_lines else '| no imputed rows | 0 |'}

`distance_raw` is always preserved. `distance_filled` equals it for observed values, contains a validated median for imputed values, and remains NULL when no hierarchy group reaches minimum support. `distance_is_imputed` and `distance_imputation_level` provide provenance.
"""
        (DOCS_DIR / "distance_imputation_report.md").write_text(distance_report, encoding="utf-8")

        manifest_tables = []
        grains = {
            "dim_date": "one row per valid calendar day",
            "dim_hour": "one row per hour of day",
            "dim_user": "one row per user",
            "dim_user_location": "one row per user location combination",
            "dim_platform": "one row per site_name and posa_continent combination",
            "dim_destination": "one row per destination ID",
            "dim_destination_type": "one row per destination type ID",
            "dim_hotel_market": "one row per hotel market attribute combination",
            "dim_hotel_cluster": "one row per hotel cluster",
            "dim_search_params": "one row per search parameter combination",
            "fct_event": "one unique aggregated source log row after exact deduplication",
            "fct_booking": "one confirmed booking log event",
            "ref_distance_stats": "one median estimator per distance hierarchy group",
        }
        primary_keys = {
            "dim_date": ["date_key"], "dim_hour": ["hour_key"], "dim_user": ["user_id"],
            "dim_user_location": ["user_location_id"], "dim_platform": ["platform_id"],
            "dim_destination": ["destination_id"], "dim_destination_type": ["destination_type_id"],
            "dim_hotel_market": ["hotel_market_id"], "dim_hotel_cluster": ["hotel_cluster_id"],
            "dim_search_params": ["search_params_id"], "fct_event": ["event_id"],
            "fct_booking": ["booking_id"],
            "ref_distance_stats": ["imputation_level", "origin_city", "origin_region", "origin_country", "destination_id", "hotel_market", "hotel_country"],
        }
        foreign_keys = {
            "fct_event": ["user_id→dim_user.user_id", "platform_id→dim_platform.platform_id", "destination_id→dim_destination.destination_id", "hotel_market_id→dim_hotel_market.hotel_market_id", "hotel_cluster_id→dim_hotel_cluster.hotel_cluster_id", "search_params_id→dim_search_params.search_params_id", "event_date_key/checkin_date_key/checkout_date_key→dim_date.date_key", "event_hour_key→dim_hour.hour_key"],
            "fct_booking": ["event_id→fct_event.event_id"],
        }
        for table in core_tables:
            manifest_tables.append({
                "table_name": f"core.{table}",
                "layer": "core",
                "grain": grains[table],
                "primary_key": primary_keys[table],
                "foreign_keys": foreign_keys.get(table, []),
                "row_count": pk_checks[table]["row_count"],
                "source_tables": ["staging.interaction", "staging.destinations"] if table in {"dim_destination", "ref_distance_stats"} else ["core.fct_event"] if table == "fct_booking" else ["staging.interaction"],
                "build_timestamp": BUILD_TS,
                "description": grains[table],
                "parquet_path": str((CORE_DIR / f"{table}.parquet").relative_to(ROOT)),
            })
        manifest = {
            "manifest_version": 1,
            "build_timestamp": BUILD_TS,
            "architecture": "RAW -> STAGING -> CORE",
            "raw_immutable": True,
            "staging": {
                "tables": [
                    {"table_name": "staging.interaction", "grain": "one source interaction row", "row_count": staging_row_count, "parquet_path": str(staging_interaction_path.relative_to(ROOT))},
                    {"table_name": "staging.destinations", "grain": "one source destination row", "row_count": raw_check[2], "parquet_path": str(staging_destinations_path.relative_to(ROOT))},
                ],
                "duplicates_flagged": staging_duplicate_count,
                "duplicates_removed_in_core": staging_row_count - core_base_count,
            },
            "mapping_checks": {
                "user_location_violations": user_location_violations,
                "site_platform_violations": site_platform_violations,
                "hotel_market_violations": hotel_market_violations,
                "destination_type_violations": destination_type_violations,
                "dim_user_location_created": user_location_unstable,
            },
            "distance": {
                "minimum_support": MIN_SUPPORT,
                "holdout_modulus": HOLDOUT_MODULUS,
                "used_levels": list(USED_DISTANCE_LEVELS),
                "raw_missing_rows": int(distance_summary[1]),
                "final_missing_rows": int(distance_summary[2]),
                "imputed_rows": int(distance_summary[3]),
                "validation": validation_records,
            },
            "validation": {
                "pk_checks": pk_checks,
                "fk_orphan_counts": fk_checks,
                "fanout_check": fanout_check,
                "all_fk_checks_pass": all(value == 0 for value in fk_checks.values()),
                "all_pk_checks_pass": all(value["pass"] for value in pk_checks.values()),
                "quality_counts_staging": dict(zip(["rows", "q_missing_checkin", "q_missing_checkout", "q_checkin_before_event", "q_checkout_before_checkin", "q_same_day_stay", "q_zero_adults", "q_zero_rooms", "q_zero_travelers", "q_extreme_future_date", "q_exact_duplicate"], quality_counts)),
            },
            "tables": manifest_tables,
        }
        (ARTIFACTS_DIR / "core_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )

        con.execute(
            "CREATE OR REPLACE VIEW meta.core_build_manifest AS "
            f"SELECT * FROM read_json_auto({output_path(ARTIFACTS_DIR / 'core_manifest.json')})"
        )
        print(json.dumps({
            "build_timestamp": BUILD_TS,
            "staging_rows": staging_row_count,
            "core_event_rows": core_base_count,
            "duplicates_removed": staging_row_count - core_base_count,
            "user_location_dimension": user_location_unstable,
            "distance_min_support": MIN_SUPPORT,
            "distance_final_missing": int(distance_summary[2]),
            "pk_checks_pass": all(value["pass"] for value in pk_checks.values()),
            "fk_checks_pass": all(value == 0 for value in fk_checks.values()),
            "fanout_check_pass": fanout_check["pass"],
        }, ensure_ascii=False, indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()
