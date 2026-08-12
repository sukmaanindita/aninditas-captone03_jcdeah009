"""
Unit tests for the Data Mart layer:
  - sql/create_dim_locations.sql
  - sql/create_mart_daily_revenue.sql
  - sql/create_mart_zone_performance.sql
  - sql/create_mart_hourly_demand.sql
  - dags/scripts/data_mart.py (pure SQL-string builders only)

Scope: pure/local template-rendering + string-builder checks only. No GCS or
BigQuery calls are made or required to run these tests -- same pattern as
tests/test_curation_transform.py: validate the *rendered* SQL string after
`.format()` substitution, not the live BigQuery execution.

Rules this file guards (per project requirement):
  - Trip/event fact data for the mart MUST come from `green_tripdata_curated`
    (curated layer), never staging.
  - Reference/master data (taxi zone lookup) is an approved, documented
    EXCEPTION: it may come directly from the staging zone lookup table,
    consistent with the Capstone 2 pattern -- it is not trip/event data and
    never goes through event-level curation.
  - All date/time aggregation uses `pickup_datetime`, never `ingestion_time`.
  - No `data_source` filter anywhere in the mart SQL -- BATCH and STREAMING
    rows must both be included automatically.
  - `CREATE OR REPLACE TABLE` (full atomic replace) is used everywhere, for
    rerun-safety without incremental/MERGE logic.
  - Partitioning/clustering clauses are present where relevant.

Run with:
    python -m unittest tests.test_data_mart_transform -v
or:
    python -m unittest discover -s tests -v
"""
import re
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "dags" / "scripts"

sys.path.insert(0, str(SCRIPTS_DIR))

# data_mart.py imports `from google.cloud import bigquery` at module level.
# Stub it out if the cloud SDK isn't installed locally, same approach as
# tests/test_batch_identity.py, so these tests run with zero cloud
# dependency / zero network access.
if 'google.cloud' not in sys.modules:
    try:
        from google.cloud import bigquery as _bq  # noqa: F401
    except ImportError:
        google_mod = types.ModuleType('google')
        google_cloud_mod = types.ModuleType('google.cloud')
        google_cloud_mod.bigquery = MagicMock()
        google_mod.cloud = google_cloud_mod
        sys.modules['google'] = google_mod
        sys.modules['google.cloud'] = google_cloud_mod
        sys.modules['google.cloud.bigquery'] = google_cloud_mod.bigquery

import data_mart  # noqa: E402


def load_raw_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text()


def strip_sql_comments(sql: str) -> str:
    """Drops full-line `--` comments so semantic checks (e.g. 'does the
    query reference ingestion_time') aren't tripped up by prose in the
    header comment block that merely *mentions* a keyword (e.g. explaining
    why ingestion_time is NOT used, or that curated's own MERGE guarantees
    uniqueness). Only whole comment lines are stripped; nothing on a code
    line is touched."""
    return "\n".join(
        line for line in sql.splitlines() if not line.strip().startswith("--")
    )


def extract_create_table_header(rendered_sql: str) -> str:
    """Returns the DDL header -- from 'CREATE OR REPLACE TABLE' up to (not
    including) the outer 'SELECT' -- so PARTITION BY / CLUSTER BY table
    options can be checked without false-matching an unrelated
    `OVER (PARTITION BY ...)` window function inside the SELECT body."""
    match = re.search(r"CREATE OR REPLACE TABLE.*?(?=SELECT)", rendered_sql, re.DOTALL)
    assert match is not None, "Could not find 'CREATE OR REPLACE TABLE ... SELECT' header"
    return match.group(0)


DUMMY_KWARGS_DIM_LOCATIONS = dict(
    mart_table="proj.cp3_aninditas_mart.dim_locations",
    staging_zone_table="proj.cp3_aninditas_staging.taxi_zone_mapping_staging",
)

DUMMY_KWARGS_DAILY_REVENUE = dict(
    mart_table="proj.cp3_aninditas_mart.mart_daily_revenue",
    curated_table="proj.cp3_aninditas_curated.green_tripdata_curated",
    dim_locations_table="proj.cp3_aninditas_mart.dim_locations",
)

DUMMY_KWARGS_ZONE_PERFORMANCE = dict(
    mart_table="proj.cp3_aninditas_mart.mart_zone_performance",
    curated_table="proj.cp3_aninditas_curated.green_tripdata_curated",
    dim_locations_table="proj.cp3_aninditas_mart.dim_locations",
)

DUMMY_KWARGS_HOURLY_DEMAND = dict(
    mart_table="proj.cp3_aninditas_mart.mart_hourly_demand",
    curated_table="proj.cp3_aninditas_curated.green_tripdata_curated",
)


class TestSqlFilesExist(unittest.TestCase):
    def test_all_four_mart_sql_files_exist_and_are_nonempty(self):
        for filename in [
            "create_dim_locations.sql",
            "create_mart_daily_revenue.sql",
            "create_mart_zone_performance.sql",
            "create_mart_hourly_demand.sql",
        ]:
            with self.subTest(filename=filename):
                raw_sql = load_raw_sql(filename)
                self.assertGreater(len(raw_sql), 0)


class TestFormatSubstitutionDoesNotRaise(unittest.TestCase):
    def test_dim_locations_format_does_not_raise(self):
        rendered = load_raw_sql("create_dim_locations.sql").format(**DUMMY_KWARGS_DIM_LOCATIONS)
        self.assertIsInstance(rendered, str)
        self.assertNotIn("{mart_table}", rendered)
        self.assertNotIn("{staging_zone_table}", rendered)

    def test_daily_revenue_format_does_not_raise(self):
        rendered = load_raw_sql("create_mart_daily_revenue.sql").format(**DUMMY_KWARGS_DAILY_REVENUE)
        self.assertIsInstance(rendered, str)
        for placeholder in ["{mart_table}", "{curated_table}", "{dim_locations_table}"]:
            self.assertNotIn(placeholder, rendered)

    def test_zone_performance_format_does_not_raise(self):
        rendered = load_raw_sql("create_mart_zone_performance.sql").format(**DUMMY_KWARGS_ZONE_PERFORMANCE)
        self.assertIsInstance(rendered, str)
        for placeholder in ["{mart_table}", "{curated_table}", "{dim_locations_table}"]:
            self.assertNotIn(placeholder, rendered)

    def test_hourly_demand_format_does_not_raise(self):
        rendered = load_raw_sql("create_mart_hourly_demand.sql").format(**DUMMY_KWARGS_HOURLY_DEMAND)
        self.assertIsInstance(rendered, str)
        for placeholder in ["{mart_table}", "{curated_table}"]:
            self.assertNotIn(placeholder, rendered)


class TestSourcingRules(unittest.TestCase):
    """Trip/event fact marts MUST source from curated only. dim_locations is
    the one documented exception (reference/master data from staging)."""

    def test_dim_locations_sources_from_staging_zone_table_only(self):
        rendered = load_raw_sql("create_dim_locations.sql").format(**DUMMY_KWARGS_DIM_LOCATIONS)
        self.assertIn(DUMMY_KWARGS_DIM_LOCATIONS["staging_zone_table"], rendered)
        # must not reference any curated table name in actual SQL (comments
        # explaining the exception are allowed to mention the name)
        code_only = strip_sql_comments(rendered)
        self.assertNotIn("green_tripdata_curated", code_only)
        self.assertNotIn("cp3_aninditas_curated", code_only)

    def test_daily_revenue_sources_trip_data_from_curated_only(self):
        rendered = load_raw_sql("create_mart_daily_revenue.sql").format(**DUMMY_KWARGS_DAILY_REVENUE)
        self.assertIn(DUMMY_KWARGS_DAILY_REVENUE["curated_table"], rendered)
        # must NOT read trip/event data directly from staging
        self.assertNotIn("taxi_zone_mapping_staging", rendered)
        self.assertNotIn("cp3_aninditas_staging", rendered)

    def test_zone_performance_sources_trip_data_from_curated_only(self):
        rendered = load_raw_sql("create_mart_zone_performance.sql").format(**DUMMY_KWARGS_ZONE_PERFORMANCE)
        self.assertIn(DUMMY_KWARGS_ZONE_PERFORMANCE["curated_table"], rendered)
        self.assertNotIn("taxi_zone_mapping_staging", rendered)
        self.assertNotIn("cp3_aninditas_staging", rendered)

    def test_hourly_demand_sources_trip_data_from_curated_only(self):
        rendered = load_raw_sql("create_mart_hourly_demand.sql").format(**DUMMY_KWARGS_HOURLY_DEMAND)
        self.assertIn(DUMMY_KWARGS_HOURLY_DEMAND["curated_table"], rendered)
        self.assertNotIn("taxi_zone_mapping_staging", rendered)
        self.assertNotIn("cp3_aninditas_staging", rendered)


class TestBatchAndStreamingCoverage(unittest.TestCase):
    """No mart query may filter on data_source -- both BATCH and STREAMING
    rows already landed in curated must flow through untouched."""

    def test_daily_revenue_does_not_filter_on_data_source(self):
        raw_sql = load_raw_sql("create_mart_daily_revenue.sql")
        # data_source may legitimately appear inside COUNTIF(...) breakdown
        # columns -- what must NOT happen is using it as a WHERE/AND filter
        # that would exclude one source's rows from the aggregation.
        self.assertNotIn("WHERE c.data_source", raw_sql)
        self.assertNotIn("AND c.data_source", raw_sql)
        # breakdown columns should still exist for visibility
        self.assertIn("COUNTIF(c.data_source = 'STREAMING')", raw_sql)
        self.assertIn("COUNTIF(c.data_source = 'BATCH')", raw_sql)

    def test_zone_performance_does_not_filter_on_data_source(self):
        raw_sql = load_raw_sql("create_mart_zone_performance.sql")
        code_only = strip_sql_comments(raw_sql)
        self.assertNotIn("data_source", code_only)

    def test_hourly_demand_does_not_filter_on_data_source(self):
        raw_sql = load_raw_sql("create_mart_hourly_demand.sql")
        code_only = strip_sql_comments(raw_sql)
        self.assertNotIn("data_source", code_only)


class TestBusinessTimeFieldUsage(unittest.TestCase):
    """Aggregation must key off pickup_datetime (business trip time), not
    ingestion_time, except where ingestion_time is explicitly needed for
    lineage (not the case in any current mart table)."""

    def test_daily_revenue_uses_pickup_datetime_not_ingestion_time(self):
        raw_sql = load_raw_sql("create_mart_daily_revenue.sql")
        code_only = strip_sql_comments(raw_sql)
        self.assertIn("c.pickup_datetime", code_only)
        self.assertNotIn("ingestion_time", code_only)

    def test_zone_performance_uses_pickup_and_dropoff_datetime_not_ingestion_time(self):
        raw_sql = load_raw_sql("create_mart_zone_performance.sql")
        code_only = strip_sql_comments(raw_sql)
        self.assertIn("c.pickup_datetime", code_only)
        self.assertIn("c.dropoff_datetime", code_only)
        self.assertNotIn("ingestion_time", code_only)

    def test_hourly_demand_uses_pickup_datetime_not_ingestion_time(self):
        raw_sql = load_raw_sql("create_mart_hourly_demand.sql")
        code_only = strip_sql_comments(raw_sql)
        self.assertIn("c.pickup_datetime", code_only)
        self.assertNotIn("ingestion_time", code_only)


class TestRerunSafetyAndPartitioning(unittest.TestCase):
    def test_all_four_tables_use_create_or_replace(self):
        for filename in [
            "create_dim_locations.sql",
            "create_mart_daily_revenue.sql",
            "create_mart_zone_performance.sql",
            "create_mart_hourly_demand.sql",
        ]:
            with self.subTest(filename=filename):
                raw_sql = load_raw_sql(filename)
                code_only = strip_sql_comments(raw_sql)
                self.assertIn("CREATE OR REPLACE TABLE", code_only)
                # must not use incremental MERGE/INSERT logic in actual SQL
                # (comments are allowed to reference "the curation MERGE"
                # when explaining why no extra dedup is needed here)
                self.assertNotIn("MERGE", code_only)
                self.assertNotIn("WHEN MATCHED", code_only)

    def test_daily_revenue_is_partitioned_and_clustered(self):
        raw_sql = load_raw_sql("create_mart_daily_revenue.sql")
        header = extract_create_table_header(raw_sql)
        self.assertIn("PARTITION BY trip_date", header)
        self.assertIn("CLUSTER BY pickup_borough, pickup_zone", header)

    def test_zone_performance_is_clustered_not_partitioned(self):
        raw_sql = load_raw_sql("create_mart_zone_performance.sql")
        header = extract_create_table_header(raw_sql)
        self.assertIn("CLUSTER BY pickup_borough, dropoff_borough", header)
        self.assertNotIn("PARTITION BY", header)

    def test_hourly_demand_is_clustered_not_partitioned(self):
        raw_sql = load_raw_sql("create_mart_hourly_demand.sql")
        header = extract_create_table_header(raw_sql)
        self.assertIn("CLUSTER BY day_of_week_num", header)
        self.assertNotIn("PARTITION BY", header)

    def test_dim_locations_has_no_partition_or_cluster(self):
        # small reference table -- partition/cluster not relevant. Its
        # dedup logic legitimately uses a window function
        # (`OVER (PARTITION BY location_id)`), which is NOT a table-level
        # PARTITION BY clause -- restrict the check to the DDL header only.
        raw_sql = load_raw_sql("create_dim_locations.sql")
        header = extract_create_table_header(raw_sql)
        self.assertNotIn("PARTITION BY", header)
        self.assertNotIn("CLUSTER BY", header)


class TestNoYearMonthHardcoding(unittest.TestCase):
    """The Data Mart deliberately does NOT take a year/month parameter (full
    cross-period rebuild every run) -- guard against any hardcoded period
    creeping in."""

    def test_no_mart_sql_file_hardcodes_a_specific_year_or_month_filter(self):
        for filename in [
            "create_dim_locations.sql",
            "create_mart_daily_revenue.sql",
            "create_mart_zone_performance.sql",
            "create_mart_hourly_demand.sql",
        ]:
            with self.subTest(filename=filename):
                raw_sql = load_raw_sql(filename)
                self.assertNotIn("{year}", raw_sql)
                self.assertNotIn("{month}", raw_sql)
                self.assertNotIn("EXTRACT(YEAR FROM", raw_sql)
                self.assertNotIn("EXTRACT(MONTH FROM", raw_sql)


class TestDimLocationsDedupAndNullGuards(unittest.TestCase):
    def test_dim_locations_excludes_null_location_id(self):
        raw_sql = load_raw_sql("create_dim_locations.sql")
        self.assertIn("WHERE location_id IS NOT NULL", raw_sql)

    def test_dim_locations_dedups_on_location_id(self):
        raw_sql = load_raw_sql("create_dim_locations.sql")
        self.assertIn("ROW_NUMBER() OVER (PARTITION BY CAST(location_id AS INT64))", raw_sql)
        self.assertIn("WHERE row_num = 1", raw_sql)


class TestKeyCheckSqlBuilders(unittest.TestCase):
    """Pure string-builder functions in data_mart.py -- no BigQuery call."""

    def test_build_null_key_check_sql_single_column(self):
        sql = data_mart.build_null_key_check_sql("proj.ds.t", ["location_id"])
        self.assertIn("WHERE location_id IS NULL", sql)
        self.assertIn("FROM `proj.ds.t`", sql)

    def test_build_null_key_check_sql_multiple_columns(self):
        sql = data_mart.build_null_key_check_sql("proj.ds.t", ["trip_date", "pickup_zone"])
        self.assertIn("trip_date IS NULL OR pickup_zone IS NULL", sql)

    def test_build_null_key_check_sql_rejects_empty_key_columns(self):
        with self.assertRaises(ValueError):
            data_mart.build_null_key_check_sql("proj.ds.t", [])

    def test_build_duplicate_key_check_sql_single_column(self):
        sql = data_mart.build_duplicate_key_check_sql("proj.ds.t", ["location_id"])
        self.assertIn("GROUP BY location_id", sql)
        self.assertIn("HAVING COUNT(*) > 1", sql)

    def test_build_duplicate_key_check_sql_multiple_columns(self):
        sql = data_mart.build_duplicate_key_check_sql(
            "proj.ds.t", ["day_of_week_num", "hour_of_day"]
        )
        self.assertIn("GROUP BY day_of_week_num, hour_of_day", sql)
        self.assertIn("day_of_week_num, hour_of_day, COUNT(*) AS cnt", sql)

    def test_build_duplicate_key_check_sql_rejects_empty_key_columns(self):
        with self.assertRaises(ValueError):
            data_mart.build_duplicate_key_check_sql("proj.ds.t", [])


class TestMartTableKeyColumnsMatchGrain(unittest.TestCase):
    """MART_TABLE_KEY_COLUMNS in data_mart.py must match the GROUP BY grain
    declared in each sql/create_*.sql file (regression guard against the two
    drifting apart)."""

    def test_dim_locations_key_columns(self):
        self.assertEqual(data_mart.MART_TABLE_KEY_COLUMNS["dim_locations"], ["location_id"])

    def test_daily_revenue_key_columns(self):
        self.assertEqual(
            data_mart.MART_TABLE_KEY_COLUMNS["mart_daily_revenue"],
            ["trip_date", "pickup_borough", "pickup_zone"],
        )

    def test_zone_performance_key_columns(self):
        self.assertEqual(
            data_mart.MART_TABLE_KEY_COLUMNS["mart_zone_performance"],
            ["pickup_zone", "pickup_borough", "dropoff_zone", "dropoff_borough"],
        )

    def test_hourly_demand_key_columns(self):
        self.assertEqual(
            data_mart.MART_TABLE_KEY_COLUMNS["mart_hourly_demand"],
            ["day_of_week_num", "hour_of_day"],
        )

    def test_every_mart_table_declared_in_sql_has_key_columns_defined(self):
        expected_tables = {
            "dim_locations",
            "mart_daily_revenue",
            "mart_zone_performance",
            "mart_hourly_demand",
        }
        self.assertEqual(set(data_mart.MART_TABLE_KEY_COLUMNS.keys()), expected_tables)


if __name__ == "__main__":
    unittest.main()
