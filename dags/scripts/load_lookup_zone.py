import io
import os
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery

# Resolving Project Root & Load Environment Variables
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

if ENV_FILE.exists():
    load_dotenv(dotenv_path=ENV_FILE)

# Murni membaca environment variables dari .env
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET_STAGING = os.environ.get("BQ_DATASET_STAGING")
BQ_DATASET_LOCATION = os.environ.get("BQ_DATASET_LOCATION")
BQ_STAGING_TABLE_TAXI_ZONE_LOOKUP = os.environ.get("BQ_STAGING_TABLE_TAXI_ZONE_LOOKUP")

LOOKUP_CSV_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def ensure_dataset(client: bigquery.Client, dataset_name: str) -> None:
    """Memastikan dataset BigQuery sudah ada."""
    dataset_id = f"{client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = BQ_DATASET_LOCATION
    client.create_dataset(dataset, exists_ok=True)


def load_taxi_zone_mapping() -> None:
    """Mengunduh CSV Taxi Zone Lookup dan mengunggah ke tabel BigQuery staging."""
    required_env_vars = {
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "BQ_DATASET_STAGING": BQ_DATASET_STAGING,
        "BQ_DATASET_LOCATION": BQ_DATASET_LOCATION,
        "BQ_STAGING_TABLE_TAXI_ZONE_LOOKUP": BQ_STAGING_TABLE_TAXI_ZONE_LOOKUP,
    }

    missing_vars = [key for key, val in required_env_vars.items() if not val]
    if missing_vars:
        raise EnvironmentError(f"Variabel berikut belum diatur di .env: {', '.join(missing_vars)}")

    client = bigquery.Client(project=GCP_PROJECT_ID)
    ensure_dataset(client, BQ_DATASET_STAGING)

    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.{BQ_STAGING_TABLE_TAXI_ZONE_LOOKUP}"

    print(f"Mengunduh file Taxi Zone Lookup dari: {LOOKUP_CSV_URL}")
    response = requests.get(LOOKUP_CSV_URL)
    response.raise_for_status()

    # Read CSV ke DataFrame
    df = pd.read_csv(io.BytesIO(response.content))

    # Standardisasi Nama Kolom & Timestamp WIB
    df.columns = df.columns.str.lower()
    df = df.rename(columns={'locationid': 'location_id'})  # <-- Tambahkan baris ini
    df['loaded_at'] = pd.Timestamp.now('Asia/Jakarta').tz_localize(None)

    # Standardisasi Tipe Data
    if 'location_id' in df.columns:
        df['location_id'] = df['location_id'].astype("Int64")

    # Mode WRITE_TRUNCATE agar data master selalu up-to-date saat di-run ulang
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    print(f"Memuat {len(df)} baris ke tabel BigQuery: {table_id}")
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()

    print(f"Sukses memuat data ke tabel: {table_id}")


def task_load_taxi_zone(**kwargs) -> None:
    """Airflow Task Wrapper"""
    load_taxi_zone_mapping()


if __name__ == "__main__":
    load_taxi_zone_mapping()