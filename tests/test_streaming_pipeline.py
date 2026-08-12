"""
Unit tests for the Beam streaming pipeline's pure logic layer
(dags/scripts/streaming_pipeline.py): parse_event, validate_event,
transform_event, process_message.

Scope: pure/local logic only. These tests do NOT import apache_beam, do NOT
require the .venv-beam environment, and do NOT connect to Pub/Sub or
BigQuery -- streaming_pipeline.py is deliberately designed so that importing
it (and calling parse_event/validate_event/transform_event/process_message)
never triggers `import apache_beam` or any network/cloud call. Only
run_pipeline()/main(), which this test file never calls, do that.

Run with:
    python -m unittest tests.test_streaming_pipeline -v
or:
    python -m unittest discover -s tests -v
"""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags" / "scripts"))

from streaming_pipeline import (  # noqa: E402
    CONTROL_FIELD,
    END_OF_STREAM_SIGNAL,
    EXPECTED_COUNT_FIELD,
    ParseError,
    StreamCompletionTracker,
    ValidationError,
    is_end_of_stream_signal,
    parse_event,
    process_message,
    transform_event,
    validate_event,
)

SAMPLE_VALID_EVENT = {
    "event_id": "00000000-0000-0000-0000-000000000001",
    "event_time": "2026-06-01T08:00:00Z",
    "ingestion_time": "2026-06-01T08:00:03Z",
    "vendor_id": "2",
    "pickup_datetime": "2026-06-01T08:00:00Z",
    "dropoff_datetime": "2026-06-01T08:15:00Z",
    "store_and_fwd_flag": "N",
    "ratecode_id": 1,
    "pickup_location_id": 42,
    "dropoff_location_id": 75,
    "passenger_count": 1,
    "trip_distance": 3.2,
    "fare_amount": 12.5,
    "extra": 0.5,
    "mta_tax": 0.5,
    "tip_amount": 2.0,
    "tolls_amount": 0.0,
    "ehail_fee": None,
    "improvement_surcharge": 0.3,
    "total_amount": 15.8,
    "payment_type": 1,
    "trip_type": 1,
    "congestion_surcharge": 0.0,
}


class TestParseEvent(unittest.TestCase):
    def test_valid_json_bytes_are_parsed_to_dict(self):
        raw = json.dumps(SAMPLE_VALID_EVENT).encode("utf-8")
        result = parse_event(raw)
        self.assertEqual(result, SAMPLE_VALID_EVENT)

    def test_valid_json_str_is_parsed_to_dict(self):
        raw = json.dumps(SAMPLE_VALID_EVENT)
        result = parse_event(raw)
        self.assertEqual(result, SAMPLE_VALID_EVENT)

    def test_malformed_json_raises_parse_error(self):
        with self.assertRaises(ParseError):
            parse_event(b'{"event_id": "abc", invalid json')

    def test_non_object_json_raises_parse_error(self):
        with self.assertRaises(ParseError):
            parse_event(b"[1, 2, 3]")

    def test_undecodable_bytes_raise_parse_error(self):
        with self.assertRaises(ParseError):
            parse_event(b"\xff\xfe\x00\x01")


class TestValidateEvent(unittest.TestCase):
    def test_valid_event_passes_without_raising(self):
        try:
            validate_event(dict(SAMPLE_VALID_EVENT))
        except ValidationError as e:
            self.fail(f"validate_event raised unexpectedly: {e.reason}")

    def test_missing_required_field_raises_validation_error(self):
        event = dict(SAMPLE_VALID_EVENT)
        del event["fare_amount"]
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_dropoff_before_pickup_raises_validation_error(self):
        event = dict(SAMPLE_VALID_EVENT)
        event["dropoff_datetime"] = "2026-06-01T07:00:00Z"  # before pickup
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_dropoff_equal_to_pickup_raises_validation_error(self):
        event = dict(SAMPLE_VALID_EVENT)
        event["dropoff_datetime"] = event["pickup_datetime"]
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_negative_trip_distance_raises_validation_error(self):
        event = dict(SAMPLE_VALID_EVENT)
        event["trip_distance"] = -1.0
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_negative_fare_amount_raises_validation_error(self):
        event = dict(SAMPLE_VALID_EVENT)
        event["fare_amount"] = -5.0
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_negative_total_amount_raises_validation_error(self):
        event = dict(SAMPLE_VALID_EVENT)
        event["total_amount"] = -5.0
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_negative_passenger_count_raises_validation_error(self):
        event = dict(SAMPLE_VALID_EVENT)
        event["passenger_count"] = -1
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_missing_pickup_location_id_raises_validation_error(self):
        event = dict(SAMPLE_VALID_EVENT)
        del event["pickup_location_id"]
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_missing_dropoff_location_id_raises_validation_error(self):
        event = dict(SAMPLE_VALID_EVENT)
        del event["dropoff_location_id"]
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_invalid_timestamp_format_raises_validation_error(self):
        event = dict(SAMPLE_VALID_EVENT)
        event["pickup_datetime"] = "not-a-timestamp"
        with self.assertRaises(ValidationError):
            validate_event(event)


class TestTransformEvent(unittest.TestCase):
    def test_numeric_string_fields_are_coerced_to_int_and_float(self):
        event = dict(SAMPLE_VALID_EVENT)
        event["passenger_count"] = "2"
        event["pickup_location_id"] = "42"
        event["trip_distance"] = "3.5"
        event["fare_amount"] = "12.50"

        result = transform_event(event)

        self.assertEqual(result["passenger_count"], 2)
        self.assertIsInstance(result["passenger_count"], int)
        self.assertEqual(result["pickup_location_id"], 42)
        self.assertIsInstance(result["pickup_location_id"], int)
        self.assertEqual(result["trip_distance"], 3.5)
        self.assertIsInstance(result["trip_distance"], float)
        self.assertEqual(result["fare_amount"], 12.50)
        self.assertIsInstance(result["fare_amount"], float)

    def test_none_values_are_preserved_not_coerced(self):
        event = dict(SAMPLE_VALID_EVENT)
        event["ehail_fee"] = None
        result = transform_event(event)
        self.assertIsNone(result["ehail_fee"])

    def test_non_numeric_fields_are_untouched(self):
        event = dict(SAMPLE_VALID_EVENT)
        result = transform_event(event)
        self.assertEqual(result["event_id"], event["event_id"])
        self.assertEqual(result["pickup_datetime"], event["pickup_datetime"])
        self.assertEqual(result["store_and_fwd_flag"], event["store_and_fwd_flag"])

    def test_transform_does_not_mutate_input(self):
        event = dict(SAMPLE_VALID_EVENT)
        event["passenger_count"] = "2"
        transform_event(event)
        self.assertEqual(event["passenger_count"], "2")


class TestProcessMessage(unittest.TestCase):
    def test_valid_message_returns_valid_status_with_record(self):
        raw = json.dumps(SAMPLE_VALID_EVENT).encode("utf-8")
        result = process_message(raw)
        self.assertEqual(result["status"], "valid")
        self.assertEqual(result["record"]["event_id"], SAMPLE_VALID_EVENT["event_id"])

    def test_malformed_json_returns_rejected_status(self):
        raw = b"{not json"
        result = process_message(raw)
        self.assertEqual(result["status"], "rejected")
        self.assertIn("Invalid JSON", result["reason"])
        self.assertEqual(result["raw_payload"], "{not json")

    def test_schema_invalid_event_returns_rejected_status(self):
        event = dict(SAMPLE_VALID_EVENT)
        del event["fare_amount"]
        raw = json.dumps(event).encode("utf-8")
        result = process_message(raw)
        self.assertEqual(result["status"], "rejected")
        self.assertIn("raw_payload", result)

    def test_domain_rule_invalid_event_returns_rejected_status(self):
        # trip_distance < 0 is already rejected by the JSON Schema's own
        # "minimum": 0.0 constraint (see event_schema.json), before the
        # supplementary domain-rule checks in validate_event() even run.
        # This still exercises the same rejected-record path end-to-end.
        event = dict(SAMPLE_VALID_EVENT)
        event["trip_distance"] = -2.0
        raw = json.dumps(event).encode("utf-8")
        result = process_message(raw)
        self.assertEqual(result["status"], "rejected")
        self.assertIn("-2.0", result["reason"])

    def test_domain_rule_only_invalid_event_returns_rejected_status(self):
        # dropoff <= pickup passes JSON Schema (no such constraint there) and
        # is only caught by the supplementary domain rule in validate_event().
        event = dict(SAMPLE_VALID_EVENT)
        event["dropoff_datetime"] = event["pickup_datetime"]
        raw = json.dumps(event).encode("utf-8")
        result = process_message(raw)
        self.assertEqual(result["status"], "rejected")
        self.assertIn("dropoff_datetime", result["reason"])


class TestIsEndOfStreamSignal(unittest.TestCase):
    def test_true_for_well_formed_eos_signal(self):
        event = {CONTROL_FIELD: END_OF_STREAM_SIGNAL, EXPECTED_COUNT_FIELD: 610}
        self.assertTrue(is_end_of_stream_signal(event))

    def test_false_for_normal_trip_event(self):
        self.assertFalse(is_end_of_stream_signal(dict(SAMPLE_VALID_EVENT)))

    def test_false_when_control_field_absent(self):
        self.assertFalse(is_end_of_stream_signal({"foo": "bar"}))

    def test_false_when_control_field_has_different_value(self):
        self.assertFalse(is_end_of_stream_signal({CONTROL_FIELD: "SOMETHING_ELSE"}))


class TestProcessMessageEndOfStream(unittest.TestCase):
    def test_well_formed_eos_signal_returns_end_of_stream_status(self):
        raw = json.dumps({CONTROL_FIELD: END_OF_STREAM_SIGNAL, EXPECTED_COUNT_FIELD: 610}).encode("utf-8")
        result = process_message(raw)
        self.assertEqual(result, {"status": "end_of_stream", "expected_count": 610})

    def test_eos_signal_never_reaches_validate_event(self):
        # A malformed trip-event-shaped payload would normally fail schema
        # validation, but an EOS signal (which lacks all trip fields) must be
        # intercepted BEFORE validate_event() runs, so it must NOT come back
        # as a generic schema-validation rejection.
        raw = json.dumps({CONTROL_FIELD: END_OF_STREAM_SIGNAL, EXPECTED_COUNT_FIELD: 0}).encode("utf-8")
        result = process_message(raw)
        self.assertEqual(result["status"], "end_of_stream")

    def test_eos_signal_missing_expected_count_is_rejected(self):
        raw = json.dumps({CONTROL_FIELD: END_OF_STREAM_SIGNAL}).encode("utf-8")
        result = process_message(raw)
        self.assertEqual(result["status"], "rejected")
        self.assertIn("Malformed END_OF_STREAM signal", result["reason"])

    def test_eos_signal_non_int_expected_count_is_rejected(self):
        raw = json.dumps({CONTROL_FIELD: END_OF_STREAM_SIGNAL, EXPECTED_COUNT_FIELD: "610"}).encode("utf-8")
        result = process_message(raw)
        self.assertEqual(result["status"], "rejected")
        self.assertIn("Malformed END_OF_STREAM signal", result["reason"])

    def test_eos_signal_boolean_expected_count_is_rejected(self):
        # bool is a subclass of int in Python -- must be explicitly excluded.
        raw = json.dumps({CONTROL_FIELD: END_OF_STREAM_SIGNAL, EXPECTED_COUNT_FIELD: True}).encode("utf-8")
        result = process_message(raw)
        self.assertEqual(result["status"], "rejected")
        self.assertIn("Malformed END_OF_STREAM signal", result["reason"])

    def test_eos_signal_negative_expected_count_is_rejected(self):
        raw = json.dumps({CONTROL_FIELD: END_OF_STREAM_SIGNAL, EXPECTED_COUNT_FIELD: -1}).encode("utf-8")
        result = process_message(raw)
        self.assertEqual(result["status"], "rejected")
        self.assertIn("Malformed END_OF_STREAM signal", result["reason"])

    def test_eos_signal_zero_expected_count_is_accepted(self):
        raw = json.dumps({CONTROL_FIELD: END_OF_STREAM_SIGNAL, EXPECTED_COUNT_FIELD: 0}).encode("utf-8")
        result = process_message(raw)
        self.assertEqual(result, {"status": "end_of_stream", "expected_count": 0})


class TestStreamCompletionTracker(unittest.TestCase):
    def test_not_completed_initially(self):
        tracker = StreamCompletionTracker()
        self.assertFalse(tracker.completed.is_set())

    def test_not_completed_with_only_processed_messages(self):
        tracker = StreamCompletionTracker()
        tracker.record_processed("key-1")
        tracker.record_processed("key-2")
        self.assertFalse(tracker.completed.is_set())

    def test_not_completed_with_only_eos_and_no_processed_messages(self):
        tracker = StreamCompletionTracker()
        tracker.record_end_of_stream(3)
        self.assertFalse(tracker.completed.is_set())

    def test_completes_once_eos_then_enough_processed_messages_arrive(self):
        tracker = StreamCompletionTracker()
        tracker.record_end_of_stream(2)
        self.assertFalse(tracker.completed.is_set())
        tracker.record_processed("key-1")
        self.assertFalse(tracker.completed.is_set())
        tracker.record_processed("key-2")
        self.assertTrue(tracker.completed.is_set())

    def test_completes_once_processed_messages_then_eos_arrives(self):
        # Order independence: EOS can arrive before or after the trip
        # messages it counts, since Pub/Sub delivery order isn't guaranteed.
        tracker = StreamCompletionTracker()
        tracker.record_processed("key-1")
        tracker.record_processed("key-2")
        self.assertFalse(tracker.completed.is_set())
        tracker.record_end_of_stream(2)
        self.assertTrue(tracker.completed.is_set())

    def test_duplicate_message_key_does_not_inflate_distinct_count(self):
        # Simulates Pub/Sub at-least-once redelivery of the same message --
        # must NOT trigger premature completion (requirement #9).
        tracker = StreamCompletionTracker()
        tracker.record_end_of_stream(2)
        tracker.record_processed("key-1")
        tracker.record_processed("key-1")  # redelivered duplicate
        tracker.record_processed("key-1")  # redelivered duplicate again
        self.assertFalse(tracker.completed.is_set())
        tracker.record_processed("key-2")
        self.assertTrue(tracker.completed.is_set())

    def test_zero_expected_count_completes_immediately_on_eos(self):
        tracker = StreamCompletionTracker()
        tracker.record_end_of_stream(0)
        self.assertTrue(tracker.completed.is_set())

    def test_more_processed_than_expected_still_completes(self):
        tracker = StreamCompletionTracker()
        tracker.record_processed("key-1")
        tracker.record_processed("key-2")
        tracker.record_processed("key-3")
        tracker.record_end_of_stream(2)
        self.assertTrue(tracker.completed.is_set())


if __name__ == "__main__":
    unittest.main()
