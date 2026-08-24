"""Export materialized Expedia marts from Parquet to CSV."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
MARTS = (
    "mart_booking_frequency",
    "mart_booking_frequency_exact",
    "mart_channel_platform",
    "mart_data_quality_daily",
    "mart_destination_performance",
    "mart_distance_quality",
    "mart_origin_destination",
    "mart_package_profile",
    "mart_product_daily",
    "mart_retention_cohort",
    "mart_session_daily",
    "mart_travel_calendar_daily",
    "mart_trip_profile",
    "mart_user_360",
)


def sql_path(path: Path) -> str:
    value = path.resolve().as_posix().replace("'", "''")
    return f"'{value}'"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=ROOT / "data" / "derived" / "marts",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "data" / "marts",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    missing = [name for name in MARTS if not (args.input_dir / f"{name}.parquet").exists()]
    if missing:
        raise FileNotFoundError("Missing Parquet marts: " + ", ".join(missing))

    con = duckdb.connect()
    try:
        for name in MARTS:
            source = args.input_dir / f"{name}.parquet"
            target = args.output_dir / f"{name}.csv"
            con.execute(
                f"COPY (SELECT * FROM read_parquet({sql_path(source)})) "
                f"TO {sql_path(target)} (HEADER, DELIMITER ',')"
            )
            print(f"exported {name}: {target}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
