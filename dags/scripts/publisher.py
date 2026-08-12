import json
import os
import random
import time
import uuid
import base64
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
PUBSUB_TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

REST_API_URL = f"https://pubsub.googleapis.com/v1/projects/{GCP_PROJECT_ID}/topics/{PUBSUB_TOPIC_ID}:publish"

VALID_GREEN_TAXI_ZONES = [7, 10, 17, 28, 33, 41, 42, 49, 55, 61, 62, 65, 80, 82, 129, 130, 260]

# Fixed (non-random) namespace for this project's streaming event ids.
# Computed once from a constant string via uuid5/NAMESPACE_DNS, so it is
# identical every time this module is imported/run -- it is NOT a random
# value. All per-record event ids are derived from this same namespace.
EVENT_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "cp3-aninditas.green-taxi-streaming")


def get_access_token():
    try:
        from google.oauth2 import service_account
        from google.auth.transport.requests import Request
        creds = service_account.Credentials.from_service_account_file(
            GOOGLE_APPLICATION_CREDENTIALS,
            scopes=["https://www.googleapis.com/auth/pubsub"]
        )
        creds.refresh(Request())
        return creds.token
    except Exception as e:
        print(f"❌ Error mendapatkan token: {e}")
        return None


def generate_realistic_trip(target_date):
    """
    Membuat isi bisnis (business payload) satu event trip sintetis untuk
    tanggal `target_date`, mengikuti domain rules NYC Green Taxi (fare model,
    payment distribution, valid pickup/dropoff zones). Periode, fare model,
    payment distribution, dan location validation TIDAK diubah dari versi
    sebelumnya -- hanya nama field yang dinormalisasi ke snake_case.

    Catatan: fungsi ini TIDAK menyertakan 'event_id'. event_id dihitung
    secara deterministic oleh compute_event_id() di start_generator(),
    setelah sequence_key (posisi record ini dalam batch harian) diketahui.
    """
    pickup_time = target_date.replace(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59)
    )
    trip_distance = round(random.uniform(0.8, 18.5), 2)
    duration_min = max(4, int(trip_distance * random.uniform(2.5, 4.0)))
    dropoff_time = pickup_time + timedelta(minutes=duration_min)

    base_fare = 2.80
    fare_amount = round(base_fare + (trip_distance * 2.50) + (duration_min * 0.50), 2)
    extra = 1.00 if 16 <= pickup_time.hour < 20 else (0.50 if pickup_time.hour >= 20 or pickup_time.hour < 6 else 0.0)

    payment_type = random.choice([1, 1, 1, 2])
    tip_amount = round(fare_amount * random.choice([0.15, 0.18, 0.20]), 2) if payment_type == 1 else 0.0
    total_amount = round(fare_amount + extra + 0.50 + 0.30 + tip_amount, 2)

    event_time_utc = pickup_time.replace(tzinfo=timezone.utc)
    ingestion_time_utc = datetime.now(timezone.utc)

    return {
        # Required Metadata Timestamps
        "event_time": event_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ingestion_time": ingestion_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),

        # Keys disesuaikan 100% dengan event_schema.json (snake_case)
        "vendor_id": str(random.choice([1, 2])),
        "pickup_datetime": pickup_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dropoff_datetime": dropoff_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "store_and_fwd_flag": "N",
        "ratecode_id": 1,
        "pickup_location_id": random.choice(VALID_GREEN_TAXI_ZONES),
        "dropoff_location_id": random.choice(VALID_GREEN_TAXI_ZONES),
        "passenger_count": random.choice([1, 1, 2, 3]),
        "trip_distance": trip_distance,
        "fare_amount": fare_amount,
        "extra": extra,
        "mta_tax": 0.50,
        "tip_amount": tip_amount,
        "tolls_amount": 0.0,
        "ehail_fee": None,
        "improvement_surcharge": 0.30,
        "total_amount": total_amount,
        "payment_type": payment_type,
        "trip_type": 1,
        "congestion_surcharge": 0.0
    }


def compute_event_id(sequence_key: str, record: dict) -> str:
    """
    Deterministic event_id = UUID5(EVENT_ID_NAMESPACE, business_key).

    business_key menggabungkan:
      - sequence_key: posisi record ini yang stabil di dalam generation run
        (tanggal + urutan harian, mis. "2026-06-01-0003"). Ini berperan
        sebagai pengganti 'source_row_number' pada batch pipeline, karena
        data Juni/Juli 2026 belum tersedia sebagai file historis nyata.
      - field bisnis inti dari record yang sudah di-generate (pickup/dropoff
        datetime, vendor, lokasi, passenger_count, trip_distance,
        fare_amount, total_amount, payment_type).

    Sifatnya:
      - Deterministic: input yang sama selalu menghasilkan event_id yang
        sama (tidak ada random/uuid4 di sini).
      - Tidak akan collide antar record berbeda dalam satu run, karena
        sequence_key sudah unik per posisi record per hari, dan berbeda
        tanggal/index selalu menghasilkan business_key yang berbeda.
    """
    parts = [
        sequence_key,
        record["pickup_datetime"],
        record["dropoff_datetime"],
        record["vendor_id"],
        str(record["pickup_location_id"]),
        str(record["dropoff_location_id"]),
        str(record["passenger_count"]),
        str(record["trip_distance"]),
        str(record["fare_amount"]),
        str(record["total_amount"]),
        str(record["payment_type"]),
    ]
    business_key = "|".join(parts)
    return str(uuid.uuid5(EVENT_ID_NAMESPACE, business_key))


def build_end_of_stream_payload(total_events: int) -> dict:
    """Payload sinyal end-of-stream (EOS) yang dikirim SEKALI setelah seluruh
    batch trip event selesai dipublish. Dikenali oleh is_end_of_stream_signal()
    di streaming_pipeline.py SEBELUM validate_event() dipanggil, sehingga
    sinyal ini tidak pernah dievaluasi terhadap event_schema.json dan tidak
    pernah masuk ke staging_stream / staging_stream_rejected. Sengaja TIDAK
    dilewatkan ke validate_payload() (bukan trip event, tidak punya field
    trip seperti pickup_datetime/fare_amount)."""
    return {"control": "END_OF_STREAM", "total_events": total_events}


def validate_payload(data: dict) -> tuple[bool, str]:
    """Pre-publish Quality Check terhadap Schema Constraint"""
    required_fields = [
        "event_id", "event_time", "ingestion_time",
        "vendor_id", "pickup_datetime", "dropoff_datetime",
        "pickup_location_id", "dropoff_location_id",
        "passenger_count", "trip_distance", "fare_amount", "total_amount", "payment_type"
    ]
    for field in required_fields:
        if data.get(field) is None:
            return False, f"Missing required schema field: '{field}'"

    if data["trip_distance"] <= 0 or data["fare_amount"] < 2.80:
        return False, "Invalid numeric values"

    return True, "OK"


def start_generator(max_per_day=10, delay_seconds=0.03):
    token = get_access_token()
    if not token:
        print("❌ Gagal mendapatkan token GCP. Pastikan credentials.json valid.")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    start_date = datetime(2026, 6, 1)
    end_date = datetime(2026, 7, 31)
    current_date = start_date
    total_published = 0
    total_failed = 0

    print(f"🚀 Generator (REST API) Aktif! Mengirim {max_per_day} data/hari...")

    try:
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            print(f"\n📅 [TANGGAL: {date_str}] Mengirim {max_per_day} data...")

            for i in range(1, max_per_day + 1):
                record = generate_realistic_trip(current_date)

                # sequence_key: posisi record yang stabil di dalam run ini,
                # dipakai sebagai bagian dari business_key untuk event_id
                # deterministic (lihat compute_event_id()).
                sequence_key = f"{date_str}-{i:04d}"
                data_dict = {"event_id": compute_event_id(sequence_key, record), **record}

                is_valid, reason = validate_payload(data_dict)
                if not is_valid:
                    total_failed += 1
                    print(f"  ⚠️ [QC FAILED] Skipping record: {reason}")
                    continue

                json_bytes = json.dumps(data_dict).encode("utf-8")
                base64_data = base64.b64encode(json_bytes).decode("utf-8")

                body = {
                    "messages": [
                        {"data": base64_data}
                    ]
                }

                response = requests.post(REST_API_URL, headers=headers, json=body)

                if response.status_code == 200:
                    total_published += 1
                    msg_id = response.json().get("messageIds", ["-"])[0]
                    print(f"  [{i}/{max_per_day}] Msg ID: {msg_id} | Pickup: {data_dict['pickup_datetime']}")
                else:
                    print(f"  ❌ Gagal kirim API Pub/Sub: {response.status_code} - {response.text}")

                time.sleep(delay_seconds)

            current_date += timedelta(days=1)

        print(f"\n✅ SELESAI! Total terkirim: {total_published} pesan. Total QC Failed: {total_failed}")

        # Kirim sinyal END_OF_STREAM SATU KALI setelah seluruh batch selesai
        # dipublish, supaya streaming_pipeline.py (StreamCompletionTracker) bisa
        # berhenti otomatis tanpa perlu Ctrl+C dari penguji. total_published
        # adalah jumlah message trip DISTINCT yang benar-benar berhasil
        # dipublish (bukan max_per_day * jumlah hari), sesuai jumlah yang akan
        # benar-benar diterima & diproses pipeline.
        eos_payload = build_end_of_stream_payload(total_published)
        eos_json_bytes = json.dumps(eos_payload).encode("utf-8")
        eos_base64_data = base64.b64encode(eos_json_bytes).decode("utf-8")
        eos_body = {"messages": [{"data": eos_base64_data}]}
        eos_response = requests.post(REST_API_URL, headers=headers, json=eos_body)
        if eos_response.status_code == 200:
            print(f"📍 Sinyal END_OF_STREAM terkirim (total_events={total_published}). Pipeline akan berhenti otomatis setelah memprosesnya.")
        else:
            print(
                f"⚠️ Gagal mengirim sinyal END_OF_STREAM: {eos_response.status_code} - {eos_response.text}. "
                "Pipeline TIDAK akan berhenti otomatis -- hentikan manual dengan Ctrl+C di sisi pipeline."
            )

    except KeyboardInterrupt:
        print(f"\n🛑 Diberhentikan. Total terkirim: {total_published} pesan.")
        print("ℹ️ Sinyal END_OF_STREAM TIDAK dikirim (batch belum selesai) -- pipeline tidak akan berhenti otomatis; hentikan manual dengan Ctrl+C di sisi pipeline.")


if __name__ == "__main__":
    start_generator(max_per_day=10, delay_seconds=0.03)
