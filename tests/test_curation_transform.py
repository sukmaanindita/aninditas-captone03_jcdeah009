"""
Unit tests for sql/transform_to_curated.sql's period parameterization and the
STREAMING branch's column mapping against the real streaming staging schema
(see sql/create_staging_stream.sql / dags/scripts/event_schema.json).

Scope: pure/local template-rendering checks only. No GCS or BigQuery calls are
made or required to run these tests -- they validate the *rendered* SQL string
after `.format()` substitution, the same pattern used by
tests/test_batch_identity.py's docstring note: "the authoritative computation
runs in BigQuery SQL, not in Python/production code."

Regression this file guards against: the STREAMING CTE branch previously
queried `{staging_stream_table}` using old batch-style column names
(lpep_pickup_datetime, vendorid, ratecodeid, pulocationid, dolocationid) that
do not exist in that table -- they belong only to the batch staging table.
That mismatch is the root cause of curation failing for June/July 2026
streaming data.

Run with:
    python -m unittest tests.test_curation_transform -v
or:
    python -m unittest discover -s tests -v
"""
import sys
import unittest
from pathlib import Path

SQL_PATH = Path(__file__).resolve().parents[1] / "sql" / "transform_to_curated.sql"

# Columns that belong ONLY to the batch-style staging table and must NEVER
# appear in the STREAMING branch (they would raise "no such field" against
# the snake_case streaming staging schema).
OLD_BATCH_STYLE_COLUMNS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "vendorid",
    "ratecodeid",
    "pulocationid",
    "dolocationid",
]

# Columns the STREAMING branch must reference, matching
# sql/create_staging_stream.sql / dags/scripts/event_schema.json exactly.
STREAMING_REQUIRED_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "vendor_id",
    "ratecode_id",
    "pickup_location_id",
    "dropoff_location_id",
]


def load_raw_sql() -> str:
    return SQL_PATH.read_text()


def split_branches(raw_sql: str) -> tuple[str, str]:
    """Split the raw (pre-format) SQL into (batch_branch, streaming_branch)
    using the section comment / UNION ALL boundaries already present in the
    file, so the two CTE branches can be asserted on independently."""
    marker_streaming = "-- 2. Data Streaming (Juni & Juli)"
    marker_end = "UNION ALL\n\n    -- 2. Data Streaming"
    assert marker_streaming in raw_sql, "STREAMING branch marker not found -- file structure changed"

    before_streaming, streaming_and_rest = raw_sql.split(marker_streaming, 1)
    # batch branch is everything before the streaming marker (includes the
    # "1. Data Batch" comment and its SELECT ... WHERE ... clause)
    batch_branch = before_streaming
    # streaming branch ends at the closing "),\n\n  transformed_data AS ("
    # of the combined_staging CTE.
    streaming_end_marker = "  transformed_data AS ("
    assert streaming_end_marker in streaming_and_rest, "transformed_data CTE marker not found"
    streaming_branch = streaming_and_rest.split(streaming_end_marker, 1)[0]
    return batch_branch, streaming_branch


class TestRawTemplateStructure(unittest.TestCase):
    """Sanity checks on the un-formatted template itself."""

    def test_sql_file_exists_and_is_nonempty(self):
        self.assertTrue(SQL_PATH.exists())
        raw_sql = load_raw_sql()
        self.assertGreater(len(raw_sql), 0)

    def test_streaming_branch_does_not_reference_old_batch_style_columns(self):
        raw_sql = load_raw_sql()
        _, streaming_branch = split_branches(raw_sql)
        for col in OLD_BATCH_STYLE_COLUMNS:
            self.assertNotIn(
                col, streaming_branch,
                f"STREAMING branch still references old batch-style column '{col}' -- "
                "this column does not exist in {staging_stream_table}."
            )

    def test_streaming_branch_references_real_streaming_schema_columns(self):
        raw_sql = load_raw_sql()
        _, streaming_branch = split_branches(raw_sql)
        for col in STREAMING_REQUIRED_COLUMNS:
            self.assertIn(
                col, streaming_branch,
                f"STREAMING branch missing expected streaming staging column '{col}'."
            )

    def test_streaming_branch_filters_period_on_pickup_datetime(self):
        raw_sql = load_raw_sql()
        _, streaming_branch = split_branches(raw_sql)
        self.assertIn("EXTRACT(YEAR FROM TIMESTAMP(pickup_datetime)) = {year}", streaming_branch)
        self.assertIn("EXTRACT(MONTH FROM TIMESTAMP(pickup_datetime)) = {month}", streaming_branch)
        # must NOT filter on ingestion_time -- pickup_datetime (business trip
        # date) is the period column, consistent with the batch branch and
        # the curated table's PARTITION BY pickup_date design.
        self.assertNotIn("EXTRACT(YEAR FROM TIMESTAMP(ingestion_time))", streaming_branch)
        self.assertNotIn("EXTRACT(MONTH FROM TIMESTAMP(ingestion_time))", streaming_branch)

    def test_batch_branch_is_unchanged_old_style_columns_still_present(self):
        # Regression guard: this fix must NOT touch the batch branch at all.
        raw_sql = load_raw_sql()
        batch_branch, _ = split_branches(raw_sql)
        for col in ["lpep_pickup_datetime", "lpep_dropoff_datetime", "vendorid", "pulocationid", "dolocationid"]:
            self.assertIn(col, batch_branch)

    def test_batch_branch_filters_period_on_lpep_pickup_datetime(self):
        raw_sql = load_raw_sql()
        batch_branch, _ = split_branches(raw_sql)
        self.assertIn("EXTRACT(YEAR FROM lpep_pickup_datetime) = {year}", batch_branch)
        self.assertIn("EXTRACT(MONTH FROM lpep_pickup_datetime) = {month}", batch_branch)


class TestFormatSubstitution(unittest.TestCase):
    """Renders the template with dummy table names across several periods
    (streaming months, a non-streaming batch month, edge months) and checks
    substitution succeeds and produces the expected values -- proving the
    query is fully parameterized, not hardcoded to June/July."""

    FORMAT_KWARGS = dict(
        curated_table="proj.ds.green_tripdata_curated",
        staging_table="proj.ds.green_tripdata_staging_batch",
        staging_stream_table="proj.ds.green_tripdata_staging_stream",
        taxi_zone_table="proj.ds.taxi_zone_mapping_staging",
    )

    PERIODS_TO_TEST = [
        (2025, 4),   # batch: April
        (2025, 5),   # batch: May
        (2026, 6),   # streaming: June
        (2026, 7),   # streaming: July
        (2026, 12),  # arbitrary non-configured month -- must still render, no hardcoding
    ]

    def test_format_does_not_raise_for_any_tested_period(self):
        raw_sql = load_raw_sql()
        for year, month in self.PERIODS_TO_TEST:
            with self.subTest(year=year, month=month):
                rendered = raw_sql.format(year=year, month=month, **self.FORMAT_KWARGS)
                self.assertIsInstance(rendered, str)
                self.assertGreater(len(rendered), 0)

    def test_year_and_month_substituted_in_both_branches_for_each_period(self):
        raw_sql = load_raw_sql()
        for year, month in self.PERIODS_TO_TEST:
            with self.subTest(year=year, month=month):
                rendered = raw_sql.format(year=year, month=month, **self.FORMAT_KWARGS)
                batch_branch, streaming_branch = split_branches(rendered)

                self.assertIn(f"= {year}", batch_branch)
                self.assertIn(f"= {month}", batch_branch)
                self.assertIn(f"= {year}", streaming_branch)
                self.assertIn(f"= {month}", streaming_branch)

    def test_no_unresolved_placeholders_remain_after_format(self):
        raw_sql = load_raw_sql()
        year, month = 2026, 6
        rendered = raw_sql.format(year=year, month=month, **self.FORMAT_KWARGS)
        # every {placeholder} in the source should have been substituted;
        # only literal braces (none expected) would remain.
        self.assertNotIn("{year}", rendered)
        self.assertNotIn("{month}", rendered)
        self.assertNotIn("{staging_table}", rendered)
        self.assertNotIn("{staging_stream_table}", rendered)
        self.assertNotIn("{taxi_zone_table}", rendered)
        self.assertNotIn("{curated_table}", rendered)

    def test_rendered_streaming_table_name_appears_in_streaming_branch(self):
        raw_sql = load_raw_sql()
        rendered = raw_sql.format(year=2026, month=6, **self.FORMAT_KWARGS)
        _, streaming_branch = split_branches(rendered)
        self.assertIn(self.FORMAT_KWARGS["staging_stream_table"], streaming_branch)

    def test_rendered_batch_table_name_appears_in_batch_branch(self):
        raw_sql = load_raw_sql()
        rendered = raw_sql.format(year=2025, month=4, **self.FORMAT_KWARGS)
        batch_branch, _ = split_branches(rendered)
        self.assertIn(self.FORMAT_KWARGS["staging_table"], batch_branch)


if __name__ == "__main__":
    unittest.main()
