"""
Beam streaming pipeline: Pub/Sub -> parse -> validate -> transform -> BigQuery.

Alur: PUBSUB_SUBSCRIPTION_ID -> parse JSON -> validate (schema + domain rules)
      -> transform (type coercion) -> WriteToBigQuery(staging_stream_table)
      event invalid -> WriteToBigQuery(staging_stream_rejected_table)

Desain testability (PENTING):
- parse_event / validate_event / transform_event / process_message adalah
  fungsi Python murni -- TIDAK mengimpor apache_beam dan TIDAK menyentuh
  Pub/Sub atau BigQuery. Fungsi-fungsi ini bisa diuji offline dengan sample
  JSON lewat `tests/test_streaming_pipeline.py`, tanpa environment .venv-beam
  dan tanpa kredensial GCP.
- `import apache_beam` sengaja ditunda (lazy import) di dalam run_pipeline()/
  main(), supaya mengimpor modul ini untuk testing tidak mensyaratkan
  apache-beam terpasang sama sekali.
- p.run() HANYA dipanggil saat file ini dieksekusi langsung sebagai skrip
  (`python streaming_pipeline.py --runner DirectRunner`), tidak pernah saat
  di-import (mis. oleh test runner).

Sinyal end-of-stream (EOS) -- publisher.py mengirim batch terbatas (Juni-Juli
2026), bukan stream tak terbatas. Supaya pipeline bisa berhenti sendiri tanpa
Ctrl+C setelah seluruh batch selesai diproses:
- publisher.py mengirim SATU message tambahan setelah seluruh trip event
  terkirim: {"control": "END_OF_STREAM", "total_events": <jumlah terkirim>}.
- process_message() mengenali message ini SEBELUM validate_event() dipanggil
  (lihat is_end_of_stream_signal()), sehingga message ini TIDAK PERNAH masuk
  ke staging_stream maupun staging_stream_rejected.
- StreamCompletionTracker (pure, stdlib threading saja -- lihat kelasnya di
  bawah) menghitung message trip DISTINCT (dedup via hash payload, supaya
  redelivery Pub/Sub at-least-once tidak membuat pipeline berhenti terlalu
  cepat) yang sudah diproses. Begitu jumlahnya >= total_events dari sinyal
  EOS, tracker men-set threading.Event `completed`.
- run_pipeline() memanggil `tracker.completed.wait()` (blocking wait, BUKAN
  polling/sleep loop) lalu memanggil result.cancel() untuk mengakhiri
  pipeline secara clean. Mekanisme ini DirectRunner-only (lihat docstring
  StreamCompletionTracker) -- bukan untuk DataflowRunner.

Menjalankan pipeline (manual, oleh user):
    source .venv-beam/bin/activate
    python dags/scripts/streaming_pipeline.py --runner DirectRunner
Pipeline berhenti otomatis setelah menerima sinyal END_OF_STREAM dari
publisher.py dan seluruh event sebelumnya selesai diproses. Ctrl+C (SIGINT)
tetap bisa dipakai sebagai fallback manual jika publisher berhenti sebelum
mengirim sinyal EOS (mis. di-interrupt).
"""
import argparse
import hashlib
import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import jsonschema
from dotenv import load_dotenv

# dags/scripts/streaming_pipeline.py -> scripts -> dags -> repo root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = PROJECT_ROOT / ".env"
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)

SCHEMA_PATH = Path(__file__).resolve().parent / "event_schema.json"

_EVENT_SCHEMA: Optional[dict] = None


def load_event_schema(schema_path: Path = SCHEMA_PATH) -> dict:
    with open(schema_path, "r") as f:
        return json.load(f)


def get_event_schema() -> dict:
    """Lazy-loaded + cached. Dipisah dari load_event_schema() supaya test bisa
    inject schema custom lewat parameter `schema=` di validate_event()."""
    global _EVENT_SCHEMA
    if _EVENT_SCHEMA is None:
        _EVENT_SCHEMA = load_event_schema()
    return _EVENT_SCHEMA


# ---------------------------------------------------------------------------
# PURE FUNCTIONS -- testable offline, tanpa apache_beam, tanpa Pub/Sub/BigQuery
# ---------------------------------------------------------------------------

class ParseError(Exception):
    """Payload bukan JSON valid / tidak bisa di-decode."""


class ValidationError(Exception):
    """Payload adalah JSON valid tapi melanggar schema atau domain rule."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def parse_event(raw: Any) -> dict:
    """Decode bytes/str Pub/Sub message -> dict. Raises ParseError jika invalid."""
    try:
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
        data = json.loads(text)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ParseError(f"Invalid JSON payload: {e}")

    if not isinstance(data, dict):
        raise ParseError("Payload JSON valid tapi bukan object/dict")
    return data


def _parse_iso8601(value: str) -> datetime:
    """Publisher mengirim format ISO8601 dengan suffix 'Z' (mis. 2026-06-01T00:00:03Z)."""
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def validate_event(event: dict, schema: Optional[dict] = None) -> None:
    """
    Validasi 2 lapis. Tidak return apa-apa jika valid; raise ValidationError jika tidak.

    1. JSON Schema (event_schema.json): tipe data, required fields, minimum numerik.
    2. Domain rules tambahan yang tidak tercakup penuh oleh JSON Schema:
       - dropoff_datetime harus setelah pickup_datetime
       - trip_distance / fare_amount / total_amount >= 0
       - passenger_count valid (>= 0, tidak None)
       - pickup_location_id & dropoff_location_id wajib ada
    """
    schema = schema if schema is not None else get_event_schema()
    try:
        jsonschema.validate(instance=event, schema=schema)
    except jsonschema.ValidationError as e:
        raise ValidationError(f"Schema validation failed: {e.message}")

    try:
        pickup = _parse_iso8601(event["pickup_datetime"])
        dropoff = _parse_iso8601(event["dropoff_datetime"])
    except (KeyError, ValueError, TypeError) as e:
        raise ValidationError(f"Invalid pickup/dropoff timestamp: {e}")

    if dropoff <= pickup:
        raise ValidationError("dropoff_datetime must be after pickup_datetime")

    if event.get("trip_distance", 0) < 0:
        raise ValidationError("trip_distance must be >= 0")
    if event.get("fare_amount", 0) < 0:
        raise ValidationError("fare_amount must be >= 0")
    if event.get("total_amount", 0) < 0:
        raise ValidationError("total_amount must be >= 0")
    if event.get("passenger_count") is None or event["passenger_count"] < 0:
        raise ValidationError("passenger_count must be present and >= 0")
    if event.get("pickup_location_id") is None or event.get("dropoff_location_id") is None:
        raise ValidationError("pickup_location_id and dropoff_location_id are required")


CONTROL_FIELD = "control"
END_OF_STREAM_SIGNAL = "END_OF_STREAM"
EXPECTED_COUNT_FIELD = "total_events"


def is_end_of_stream_signal(event: dict) -> bool:
    """True jika payload JSON yang sudah di-parse adalah sinyal end-of-stream
    eksplisit dari publisher.py, BUKAN taxi trip event biasa. Dicek SEBELUM
    validate_event() dipanggil di process_message(), supaya sinyal ini tidak
    pernah dievaluasi terhadap event_schema.json (yang mensyaratkan field
    trip seperti pickup_datetime) dan tidak pernah berakhir di
    staging_stream / staging_stream_rejected."""
    return event.get(CONTROL_FIELD) == END_OF_STREAM_SIGNAL


def transform_event(event: dict) -> dict:
    """Coerce tipe numerik supaya sesuai skema BigQuery (sql/create_staging_stream.sql).
    Nama field TIDAK diubah -- sudah snake_case & identik dengan nama kolom BQ."""
    int_fields = [
        "ratecode_id", "pickup_location_id", "dropoff_location_id",
        "passenger_count", "payment_type", "trip_type",
    ]
    float_fields = [
        "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "ehail_fee", "improvement_surcharge", "total_amount",
        "congestion_surcharge",
    ]

    out = dict(event)
    for f in int_fields:
        if out.get(f) is not None:
            out[f] = int(out[f])
    for f in float_fields:
        if out.get(f) is not None:
            out[f] = float(out[f])
    return out


def process_message(raw: Any, schema: Optional[dict] = None) -> dict:
    """
    Gabungan parse -> (cek EOS) -> validate -> transform untuk satu Pub/Sub
    message. Fungsi tunggal ini yang paling praktis dites end-to-end secara
    offline (lihat tests/test_streaming_pipeline.py).

    Return:
      {"status": "valid", "record": {...}}  -- siap ditulis ke staging_stream_table
      {"status": "rejected", "reason": str, "raw_payload": str}  -- ke rejected_table
      {"status": "end_of_stream", "expected_count": int}  -- sinyal EOS dari
        publisher.py; TIDAK PERNAH ditulis ke staging_stream/rejected. Dipakai
        run_pipeline() untuk menghentikan pipeline secara clean.
    """
    try:
        event = parse_event(raw)
    except ParseError as e:
        raw_text = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
        return {"status": "rejected", "reason": str(e), "raw_payload": raw_text}

    if is_end_of_stream_signal(event):
        expected_count = event.get(EXPECTED_COUNT_FIELD)
        if not isinstance(expected_count, int) or isinstance(expected_count, bool) or expected_count < 0:
            # Sinyal EOS malformed (mis. total_events hilang/bukan int) --
            # diperlakukan sebagai rejected record biasa, bukan dipercaya
            # begitu saja sebagai completion count yang tidak bisa dipakai.
            return {
                "status": "rejected",
                "reason": f"Malformed END_OF_STREAM signal: missing/invalid '{EXPECTED_COUNT_FIELD}'",
                "raw_payload": json.dumps(event),
            }
        return {"status": "end_of_stream", "expected_count": expected_count}

    try:
        validate_event(event, schema=schema)
    except ValidationError as e:
        return {"status": "rejected", "reason": e.reason, "raw_payload": json.dumps(event)}

    return {"status": "valid", "record": transform_event(event)}


class StreamCompletionTracker:
    """Thread-safe, in-process tracker untuk mekanisme end-of-stream (EOS) yang
    hanya berlaku untuk DirectRunner (lihat catatan di bawah). Sengaja hanya
    memakai stdlib `threading` -- TIDAK mengimpor apache_beam -- supaya kelas
    ini bisa dites offline secara independen (lihat tests/test_streaming_pipeline.py).

    Kenapa harus jadi module-level global (bukan diberikan ke constructor DoFn):
    DirectRunner bisa mem-pickle/menduplikasi instance DoFn untuk isolasi antar
    bundle. threading.Lock dan threading.Event tidak aman untuk di-pickle atau
    dibagi lintas salinan seperti itu. Dengan mereferensikan tracker ini lewat
    nama module-level di dalam DoFn.process() (bukan menyimpannya sebagai
    atribut instance DoFn), setiap panggilan process() akan mengambil ulang
    objek yang sama dari namespace modul saat runtime -- ini valid karena
    DirectRunner (mode in-memory/thread default) tidak pernah keluar dari
    proses Python yang sama. Mekanisme ini TIDAK berlaku untuk DataflowRunner
    (worker terpisah/multi-proses/multi-mesin) -- di luar scope project ini.

    Logika completion: EOS dianggap tercapai begitu (a) sinyal END_OF_STREAM
    sudah diterima (memberi tahu total_events yang diharapkan) DAN (b) jumlah
    message trip DISTINCT yang sudah diproses >= total_events tersebut. Pakai
    `set()` dari hash payload (bukan counter polos) supaya redelivery Pub/Sub
    at-least-once tidak membuat pipeline berhenti sebelum semua message unik
    benar-benar selesai diproses (lihat requirement #9)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._seen_message_keys = set()
        self._expected_count: Optional[int] = None
        self.completed = threading.Event()

    def record_processed(self, message_key: str) -> None:
        """Dipanggil oleh ProcessMessageFn untuk setiap message trip (valid
        maupun rejected) yang selesai diproses -- BUKAN untuk sinyal EOS itu
        sendiri."""
        with self._lock:
            self._seen_message_keys.add(message_key)
            self._check_complete_locked()

    def record_end_of_stream(self, expected_count: int) -> None:
        """Dipanggil sekali saat sinyal END_OF_STREAM diterima."""
        with self._lock:
            self._expected_count = expected_count
            self._check_complete_locked()

    def _check_complete_locked(self) -> None:
        """Harus dipanggil dengan self._lock sudah dipegang."""
        if self._expected_count is not None and len(self._seen_message_keys) >= self._expected_count:
            self.completed.set()


_tracker: Optional[StreamCompletionTracker] = None


# ---------------------------------------------------------------------------
# BEAM PIPELINE -- apache_beam di-import lazy, hanya dipakai saat run_pipeline()
# atau main() benar-benar dipanggil (bukan saat modul ini di-import untuk test)
# ---------------------------------------------------------------------------

def run_pipeline(
    subscription: str,
    staging_table: str,
    rejected_table: str,
    project_id: Optional[str] = None,
    runner: str = "DirectRunner",
) -> None:
    """Membangun & menjalankan (p.run()) pipeline Beam. TIDAK dipanggil oleh
    import/test -- hanya lewat main() saat file dieksekusi langsung.

    Auto-termination: pipeline berhenti sendiri (tanpa Ctrl+C) setelah
    menerima sinyal END_OF_STREAM dari publisher.py dan seluruh message trip
    yang sudah dipublish sebelumnya selesai diproses. Lihat StreamCompletionTracker
    di atas untuk detail mekanisme & kenapa ini DirectRunner-only."""
    import apache_beam as beam
    from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, StandardOptions

    global _tracker
    _tracker = StreamCompletionTracker()

    options = PipelineOptions(streaming=True)
    options.view_as(StandardOptions).runner = runner
    if project_id:
        options.view_as(GoogleCloudOptions).project = project_id

    class ProcessMessageFn(beam.DoFn):
        OUTPUT_REJECTED = "rejected"

        def process(self, element):
            result = process_message(element)

            if result["status"] == "end_of_stream":
                # Sinyal kontrol -- TIDAK PERNAH di-yield ke output manapun
                # (tidak masuk staging_stream / staging_stream_rejected).
                # Referensi _tracker lewat nama module-level (bukan self.tracker)
                # -- lihat docstring StreamCompletionTracker.
                _tracker.record_end_of_stream(result["expected_count"])
                return

            message_key = hashlib.sha256(
                element if isinstance(element, (bytes, bytearray)) else str(element).encode("utf-8")
            ).hexdigest()
            _tracker.record_processed(message_key)

            if result["status"] == "valid":
                yield result["record"]
            else:
                yield beam.pvalue.TaggedOutput(
                    self.OUTPUT_REJECTED,
                    {
                        "rejected_at": datetime.now(timezone.utc).isoformat(),
                        "reason": result["reason"],
                        "raw_payload": result["raw_payload"],
                    },
                )

    p = beam.Pipeline(options=options)
    processed = (
        p
        | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=subscription)
        | "ProcessMessage" >> beam.ParDo(ProcessMessageFn()).with_outputs(
            ProcessMessageFn.OUTPUT_REJECTED, main="valid"
        )
    )

    processed.valid | "WriteValidToBQ" >> beam.io.WriteToBigQuery(
        staging_table,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
    )
    processed.rejected | "WriteRejectedToBQ" >> beam.io.WriteToBigQuery(
        rejected_table,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
    )

    result = p.run()
    logging.info("Pipeline running. Waiting for END_OF_STREAM signal from publisher...")
    _tracker.completed.wait()
    logging.info("END_OF_STREAM received and all prior events processed. Cancelling pipeline...")
    try:
        result.cancel()
    except Exception as e:
        logging.warning("result.cancel() raised (may be expected depending on Beam/runner version): %s", e)
    try:
        result.wait_until_finish()
    except Exception as e:
        logging.info("Pipeline finished (wait_until_finish raised after cancel, as expected on some versions): %s", e)
    logging.info("Pipeline stopped cleanly.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Green Taxi streaming pipeline: Pub/Sub -> validate -> BigQuery staging_stream."
    )
    parser.add_argument("--runner", default="DirectRunner", help="DirectRunner untuk lokal (default).")
    parser.add_argument("--subscription", default=None, help="Full path projects/<project>/subscriptions/<id>. Default dari .env")
    parser.add_argument("--staging-table", default=None, help="Format project:dataset.table. Default dari .env")
    parser.add_argument("--rejected-table", default=None, help="Format project:dataset.table. Default dari .env")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset = os.environ.get("BQ_DATASET_STAGING")
    staging_table_name = os.environ.get("BQ_STAGING_TABLE_STREAM")
    subscription_id = os.environ.get("PUBSUB_SUBSCRIPTION_ID")

    missing = [k for k, v in {
        "GCP_PROJECT_ID": project_id,
        "BQ_DATASET_STAGING": dataset,
        "BQ_STAGING_TABLE_STREAM": staging_table_name,
        "PUBSUB_SUBSCRIPTION_ID": subscription_id,
    }.items() if not v and not (k == "PUBSUB_SUBSCRIPTION_ID" and args.subscription)]
    if missing and not (args.subscription and args.staging_table and args.rejected_table):
        raise EnvironmentError(f"Variabel berikut belum diatur di .env: {', '.join(missing)}")

    subscription = args.subscription or f"projects/{project_id}/subscriptions/{subscription_id}"
    staging_table = args.staging_table or f"{project_id}:{dataset}.{staging_table_name}"
    rejected_table = args.rejected_table or f"{project_id}:{dataset}.{staging_table_name}_rejected"

    logging.info("Starting streaming pipeline | runner=%s | subscription=%s", args.runner, subscription)
    logging.info("Valid records -> %s | Rejected records -> %s", staging_table, rejected_table)

    run_pipeline(
        subscription=subscription,
        staging_table=staging_table,
        rejected_table=rejected_table,
        project_id=project_id,
        runner=args.runner,
    )


if __name__ == "__main__":
    main()
