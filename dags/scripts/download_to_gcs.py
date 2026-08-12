import os
import tempfile
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv
from google.cloud import storage

# Resolving Project Root & Environment Variables
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"

if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)

GCP_PROJECT = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET = os.environ.get("GCS_BUCKET_NAME")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def upload_to_gcs(year: int, month: int) -> str:
    """Mengunduh file parquet dari TLC NYC dan mengunggahnya ke GCS secara idempotent."""
    if not GCP_PROJECT or not GCS_BUCKET:
        raise EnvironmentError("GCP_PROJECT_ID dan GCS_BUCKET_NAME harus diatur di .env")

    year = int(year)
    month = int(month)

    filename = f"green_tripdata_{year}-{month:02d}.parquet"
    source_url = f"{BASE_URL}/{filename}"
    object_name = f"data/raw/green_tripdata/{year}/{month:02d}/{filename}"

    client = storage.Client(project=GCP_PROJECT)
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(object_name)

    # 1. Idempotency Check: Jangan upload jika file sudah ada di GCS
    if blob.exists():
        print(f"File {object_name} sudah ada di GCS. Skipping upload.")
        return object_name

    print(f"Downloading {source_url}...")

    # 2. Download menggunakan Stream (Hemat RAM / OOM Prevention)
    with tempfile.NamedTemporaryFile(delete=True) as temp_file:
        with requests.get(source_url, stream=True) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)
            temp_file.flush()

        print(f"Uploading {object_name} ke GCS bucket '{GCS_BUCKET}'...")
        blob.upload_from_filename(temp_file.name)
        print(f"Selesai upload {object_name} ke GCS.")

    return object_name


def task_load_to_gcs(year: Optional[str] = None, month: Optional[str] = None) -> str:
    """Airflow Task Wrapper"""
    if year is None or month is None:
        raise ValueError("Parameter year dan month wajib diberikan.")
    return upload_to_gcs(year=int(year), month=int(month))


def main():
    # Contoh eksekusi lokal untuk April dan Mei 2026
    for month in [4, 5]:
        upload_to_gcs(year=2026, month=month)


if __name__ == "__main__":
    main()