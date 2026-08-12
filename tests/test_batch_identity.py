"""
Unit tests for the batch identity strategy (source_row_number, source_file_identity,
deterministic event_id, and business-column deduplication).

Scope: pure/local logic only. No GCS or BigQuery calls are made or required to run
these tests -- they validate the Python-side helpers in dags/scripts/load_staging.py
plus a Python mirror of the SHA256 event_id formula that BigQuery executes in
sql/transform_to_curated.sql (see compute_event_id_mirror below).

Run with:
    python -m unittest tests.test_batch_identity -v
or:
    python -m unittest discover -s tests -v
"""
import hashlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags" / "scripts"))

# load_staging.py imports `from google.cloud import bigquery, storage` at module
# level for its cloud I/O functions. The functions under test here
# (assign_source_identity, deduplicate_business_rows) are pure and never touch
# those clients, so we stub the google-cloud packages if they aren't installed
# in the local/test environment -- this keeps these tests runnable without any
# cloud SDK or network access.
if 'google.cloud' not in sys.modules:
    try:
        from google.cloud import bigquery as _bq  # noqa: F401
    except ImportError:
        google_mod = types.ModuleType('google')
        google_cloud_mod = types.ModuleType('google.cloud')
        google_cloud_mod.bigquery = MagicMock()
        google_cloud_mod.storage = MagicMock()
        google_mod.cloud = google_cloud_mod
        sys.modules['google'] = google_mod
        sys.modules['google.cloud'] = google_cloud_mod
        sys.modules['google.cloud.bigquery'] = google_cloud_mod.bigquery
        sys.modules['google.cloud.storage'] = google_cloud_mod.storage

# load_staging.py also does `from google.api_core.exceptions import NotFound`
# (delete_existing_month_data() catches specifically this exception -- see
# TestDeleteExistingMonthData below). Stub it with a REAL Exception subclass
# (not a MagicMock) if google-api-core isn't installed locally, so tests can
# meaningfully raise/catch it via `except NotFound:`.
if 'google.api_core.exceptions' not in sys.modules:
    try:
        from google.api_core.exceptions import NotFound as _NotFound  # noqa: F401
    except ImportError:
        google_api_core_mod = types.ModuleType('google.api_core')
        google_api_core_exceptions_mod = types.ModuleType('google.api_core.exceptions')

        class NotFound(Exception):
            """Minimal stand-in for google.api_core.exceptions.NotFound."""

        google_api_core_exceptions_mod.NotFound = NotFound
        google_api_core_mod.exceptions = google_api_core_exceptions_mod
        # 'google' may already be in sys.modules from the block above (or
        # from a real partial install) -- reuse it rather than clobbering it.
        google_mod = sys.modules.get('google') or types.ModuleType('google')
        google_mod.api_core = google_api_core_mod
        sys.modules['google'] = google_mod
        sys.modules['google.api_core'] = google_api_core_mod
        sys.modules['google.api_core.exceptions'] = google_api_core_exceptions_mod

from load_staging import (  # noqa: E402
    BatchPipelineConfig,
    NotFound,
    assign_source_identity,
    deduplicate_business_rows,
    delete_existing_month_data,
)


def compute_event_id_mirror(
    source_file_identity,
    source_row_number,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    vendorid,
    pulocationid,
    dolocationid,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount,
    payment_type,
):
    """Python mirror of the BATCH branch event_id formula in
    sql/transform_to_curated.sql:

        TO_HEX(SHA256(CONCAT(
            IFNULL(source_file_identity, ''), '|',
            IFNULL(CAST(source_row_number AS STRING), ''), '|',
            ... business fields ...
        )))

    This mirror exists purely so the *properties* of the identity strategy
    (determinism, uniqueness) can be unit-tested locally. The authoritative
    computation runs in BigQuery SQL, not in Python/production code.
    """
    def s(value):
        return '' if value is None else str(value)

    parts = [
        s(source_file_identity),
        s(source_row_number),
        s(lpep_pickup_datetime),
        s(lpep_dropoff_datetime),
        s(vendorid),
        s(pulocationid),
        s(dolocationid),
        s(passenger_count),
        s(trip_distance),
        s(fare_amount),
        s(total_amount),
        s(payment_type),
    ]
    payload = '|'.join(parts)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


class TestSourceIdentityAssignment(unittest.TestCase):
    def test_source_row_number_is_positional_and_deterministic(self):
        df = pd.DataFrame({'fare_amount': [10.0, 20.0, 30.0]})
        result = assign_source_identity(df, source_file_identity='data/raw/x.parquet')
        self.assertEqual(list(result['source_row_number']), [0, 1, 2])

    def test_source_file_identity_is_constant_and_consistent(self):
        df = pd.DataFrame({'fare_amount': [10.0, 20.0]})
        result = assign_source_identity(df, source_file_identity='data/raw/green_tripdata/2025/04/green_tripdata_2025-04.parquet')
        self.assertTrue((result['source_file_identity'] == 'data/raw/green_tripdata/2025/04/green_tripdata_2025-04.parquet').all())

    def test_assign_source_identity_does_not_mutate_input(self):
        df = pd.DataFrame({'fare_amount': [10.0]})
        assign_source_identity(df, source_file_identity='data/raw/x.parquet')
        self.assertNotIn('source_row_number', df.columns)


class TestBusinessDeduplication(unittest.TestCase):
    def test_true_duplicate_business_rows_are_collapsed(self):
        # Two physically distinct source rows (different source_row_number) but
        # identical business content -> should be treated as a true duplicate.
        df = pd.DataFrame({
            'lpep_pickup_datetime': ['2025-04-01 08:00:00', '2025-04-01 08:00:00'],
            'pulocationid': [10, 10],
            'fare_amount': [15.0, 15.0],
            'loaded_at': ['2025-04-01', '2025-04-01'],
        })
        df = assign_source_identity(df, source_file_identity='data/raw/x.parquet')
        result = deduplicate_business_rows(df)
        self.assertEqual(len(result), 1)
        # keep='first' after sort by source_row_number -> row 0 retained.
        self.assertEqual(result.iloc[0]['source_row_number'], 0)

    def test_distinct_trips_sharing_a_narrow_key_are_not_dropped(self):
        # Same pickup_datetime/location/fare (the OLD fragile composite key) but a
        # different business field (dropoff, trip_distance) -> must NOT be collapsed.
        df = pd.DataFrame({
            'lpep_pickup_datetime': ['2025-04-01 08:00:00', '2025-04-01 08:00:00'],
            'pulocationid': [10, 10],
            'fare_amount': [15.0, 15.0],
            'trip_distance': [2.1, 5.7],
            'loaded_at': ['2025-04-01', '2025-04-01'],
        })
        df = assign_source_identity(df, source_file_identity='data/raw/x.parquet')
        result = deduplicate_business_rows(df)
        self.assertEqual(len(result), 2)

    def test_source_row_number_excluded_from_dedup_subset(self):
        # If source_row_number were included in the dedup subset, these two
        # identical-business rows would never collapse (each row_number is unique).
        df = pd.DataFrame({
            'fare_amount': [15.0, 15.0],
            'loaded_at': ['2025-04-01', '2025-04-01'],
        })
        df = assign_source_identity(df, source_file_identity='data/raw/x.parquet')
        result = deduplicate_business_rows(df)
        self.assertEqual(len(result), 1)


class TestEventIdFormula(unittest.TestCase):
    """Covers the 3 minimum cases required for the identity fix:
    1. same source row -> same event_id
    2. two different source rows -> different event_id
    3. same file + row + business fields -> deterministic (stable across calls/reruns)
    """

    BASE_ROW = dict(
        source_file_identity='data/raw/green_tripdata/2025/04/green_tripdata_2025-04.parquet',
        source_row_number=42,
        lpep_pickup_datetime='2025-04-15 08:30:00',
        lpep_dropoff_datetime='2025-04-15 08:45:00',
        vendorid=2,
        pulocationid=74,
        dolocationid=75,
        passenger_count=1,
        trip_distance=2.3,
        fare_amount=12.5,
        total_amount=15.0,
        payment_type=1,
    )

    def test_same_source_row_yields_same_event_id(self):
        id_a = compute_event_id_mirror(**self.BASE_ROW)
        id_b = compute_event_id_mirror(**self.BASE_ROW)
        self.assertEqual(id_a, id_b)

    def test_different_source_row_number_yields_different_event_id(self):
        row_a = dict(self.BASE_ROW)
        row_b = dict(self.BASE_ROW, source_row_number=43)
        self.assertNotEqual(compute_event_id_mirror(**row_a), compute_event_id_mirror(**row_b))

    def test_different_source_file_identity_yields_different_event_id(self):
        # Guards against April vs May (or any two files) colliding even if every
        # other field happens to match.
        row_april = dict(self.BASE_ROW)
        row_may = dict(
            self.BASE_ROW,
            source_file_identity='data/raw/green_tripdata/2025/05/green_tripdata_2025-05.parquet',
        )
        self.assertNotEqual(compute_event_id_mirror(**row_april), compute_event_id_mirror(**row_may))

    def test_rerun_of_same_file_row_and_fields_is_deterministic(self):
        # Simulates an Airflow rerun: identical inputs recomputed independently
        # (e.g. in a fresh process/call) must yield the identical event_id so the
        # MERGE treats it as an update, not a new insert.
        first_run = compute_event_id_mirror(**self.BASE_ROW)
        second_run = compute_event_id_mirror(**dict(self.BASE_ROW))
        self.assertEqual(first_run, second_run)

    def test_null_business_field_does_not_collapse_to_null_event_id(self):
        row = dict(self.BASE_ROW, passenger_count=None, trip_distance=None)
        event_id = compute_event_id_mirror(**row)
        self.assertIsNotNone(event_id)
        self.assertEqual(len(event_id), 64)  # SHA256 hex digest length


class TestDeleteExistingMonthData(unittest.TestCase):
    """Covers the delete_existing_month_data() fix: only NotFound (table
    doesn't exist yet) should be swallowed; every other error must propagate
    so the Airflow task fails loudly instead of silently continuing on top
    of a DELETE that may not have actually happened.

    All BigQuery interaction is mocked -- no network/cloud call is made.
    """

    BASE_CONFIG = BatchPipelineConfig(
        project_id='proj',
        bucket_name='bucket',
        staging_dataset_name='ds',
        staging_table_name='green_tripdata_staging_batch',
        year=2026,
        month=4,
    )

    def _make_client(self, result_side_effect=None):
        client = MagicMock()
        query_job = MagicMock()
        if result_side_effect is not None:
            query_job.result.side_effect = result_side_effect
        client.query.return_value = query_job
        return client, query_job

    def test_success_path_does_not_raise_and_runs_delete_query(self):
        client, query_job = self._make_client()
        delete_existing_month_data(client, self.BASE_CONFIG)
        client.query.assert_called_once()
        query_job.result.assert_called_once()
        # sanity: the DELETE targets the right table/period
        called_query = client.query.call_args[0][0]
        self.assertIn(self.BASE_CONFIG.staging_table_id, called_query)
        self.assertIn('2026', called_query)
        self.assertIn('4', called_query)

    def test_not_found_error_is_swallowed_load_continues(self):
        client, _ = self._make_client(result_side_effect=NotFound('Table not found: proj.ds.x'))
        try:
            delete_existing_month_data(client, self.BASE_CONFIG)
        except NotFound:
            self.fail("delete_existing_month_data() must swallow NotFound (table not yet created)")

    def test_permission_error_still_raises(self):
        client, _ = self._make_client(result_side_effect=PermissionError('403 access denied'))
        with self.assertRaises(PermissionError):
            delete_existing_month_data(client, self.BASE_CONFIG)

    def test_generic_runtime_error_still_raises(self):
        # Simulates e.g. a quota error, network error, or malformed-query
        # error surfaced as some other exception type -- must NOT be
        # swallowed just because it's "some exception".
        client, _ = self._make_client(result_side_effect=RuntimeError('quota exceeded'))
        with self.assertRaises(RuntimeError):
            delete_existing_month_data(client, self.BASE_CONFIG)

    def test_value_error_still_raises(self):
        client, _ = self._make_client(result_side_effect=ValueError('malformed query'))
        with self.assertRaises(ValueError):
            delete_existing_month_data(client, self.BASE_CONFIG)


if __name__ == '__main__':
    unittest.main()
