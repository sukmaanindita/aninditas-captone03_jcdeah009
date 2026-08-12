import io
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

if ENV_FILE.exists():
    load_dotenv(dotenv_path=ENV_FILE)

# Murni membaca environment variables dari .env
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
BQ_DATASET_STAGING = os.environ.get("BQ_DATASET_STAGING")
BQ_DATASET_LOCATION = os.environ.get("BQ_DATASET_LOCATION")
BQ_STAGING_TABLE_BATCH = os.environ.get("BQ_STAGING_TABLE_BATCH")


@dataclass
class BatchPipelineConfig:
    project_id: str
    bucket_name: str
    staging_dataset_name: str
    staging_table_name: str
    year: int
    month: int

    @property
    def source_uri(self) -> str:
        return (
            f"gs://{self.bucket_name}/data/raw/green_tripdata/{self.year}/{self.month:02d}/"
            f"green_tripdata_{self.year}-{self.month:02d}.parquet"
        )

    @property
    def gcs_blob_path(self) -> str:
        return f"data/raw/green_tripdata/{self.year}/{self.month:02d}/green_tripdata_{self.year}-{self.month:02d}.parquet"

    @property
    def staging_table_id(self) -> str:
        return f"{self.project_id}.{self.staging_dataset_name}.{self.staging_table_name}"


def build_config(year: int, month: int) -> BatchPipelineConfig:
    required_env_vars = {
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "GCS_BUCKET_NAME": GCS_BUCKET_NAME,
        "BQ_DATASET_STAGING": BQ_DATASET_STAGING,
        "BQ_STAGING_TABLE_BATCH": BQ_STAGING_TABLE_BATCH,
        "BQ_DATASET_LOCATION": BQ_DATASET_LOCATION,
    }
    
    missing_vars = [key for key, val in required_env_vars.items() if not val]
    if missing_vars:
        raise EnvironmentError(f"Variabel berikut belum diatur di .env: {', '.join(missing_vars)}")

    return BatchPipelineConfig(
        project_id=GCP_PROJECT_ID,
        bucket_name=GCS_BUCKET_NAME,
        staging_dataset_name=BQ_DATASET_STAGING,
        staging_table_name=BQ_STAGING_TABLE_BATCH,
        year=year,
        month=month,
    )


def ensure_dataset(client: bigquery.Client, dataset_name: str) -> None:
    dataset_id = f"{client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = BQ_DATASET_LOCATION
    client.create_dataset(dataset, exists_ok=True)


def delete_existing_month_data(client: bigquery.Client, config: BatchPipelineConfig) -> None:
    """Menghapus data periode (year, month) yang sama dari staging sebelum load ulang
    (supaya WRITE_APPEND di load_parquet_from_gcs() tidak menumpuk data lama+baru).

    HANYA mengabaikan google.api_core.exceptions.NotFound -- yaitu kasus tabel
    staging memang belum pernah dibuat (load pertama kali untuk project ini).
    Error lain (permission, quota, network, query syntax, dll.) SENGAJA tetap
    di-raise, supaya task Airflow gagal secara eksplisit alih-alih diam-diam
    melanjutkan proses load di atas kemungkinan data lama yang gagal terhapus
    (yang bisa menghasilkan duplikat/data basi untuk periode tersebut tanpa
    ada error yang terlihat).
    """
    query = f"""
        DELETE FROM `{config.staging_table_id}`
        WHERE EXTRACT(YEAR FROM lpep_pickup_datetime) = {config.year}
          AND EXTRACT(MONTH FROM lpep_pickup_datetime) = {config.month}
    """
    try:
        client.query(query).result()
        print(f"Data lama untuk periode {config.year}-{config.month:02d} berhasil dibersihkan dari staging.")
    except NotFound as e:
        print(f"Informasi: Tabel staging belum ada ({e}). Melanjutkan proses load (tabel akan dibuat oleh load job)...")


def assign_source_identity(df: pd.DataFrame, source_file_identity: str) -> pd.DataFrame:
    """Menempelkan source_row_number & source_file_identity ke DataFrame.

    HARUS dipanggil sebelum filter/dedup/transform/sort apa pun, karena
    source_row_number merepresentasikan posisi baris asli (0-based) di dalam
    file source. Fungsi ini murni (pure) dan tidak menyentuh GCS/BigQuery,
    sehingga bisa diuji langsung tanpa cloud dependency.

    - source_row_number: dipakai sebagai komponen deterministic identity untuk
      event_id (lihat transform_to_curated.sql). TIDAK dipakai sebagai satu-
      satunya kunci dedup, karena setiap baris otomatis unik secara posisi.
    - source_file_identity: identitas file source GCS (mis. config.gcs_blob_path)
      yang deterministic & reproducible untuk periode (year, month) yang sama.
    """
    df = df.copy()
    df['source_row_number'] = np.arange(len(df), dtype='int64')
    df['source_file_identity'] = source_file_identity
    return df


def deduplicate_business_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplikasi berbasis kolom bisnis (isi baris asli), bukan full-row.

    Kolom identity/metadata (source_row_number, source_file_identity,
    loaded_at) HARUS dikecualikan dari subset dedup: jika ikut disertakan,
    setiap baris akan selalu tampak "unik" (karena source_row_number unik per
    baris) sehingga duplikat fisik/source tidak pernah terdeteksi.

    source_row_number tetap berperan sebagai tie-breaker implisit lewat
    keep='first': baris dengan posisi source_row_number terkecil yang
    dipertahankan saat ada duplikat bisnis.
    """
    identity_and_metadata_columns = ['source_row_number', 'source_file_identity', 'loaded_at']
    business_columns = [c for c in df.columns if c not in identity_and_metadata_columns]

    df = df.sort_values('source_row_number')
    return df.drop_duplicates(subset=business_columns, keep='first')


def load_parquet_from_gcs(config: BatchPipelineConfig) -> None:
    bq_client = bigquery.Client(project=config.project_id)
    ensure_dataset(bq_client, config.staging_dataset_name)

    print(f"Membaca data dari GCS: {config.source_uri}")
    
    storage_client = storage.Client(project=config.project_id)
    bucket = storage_client.bucket(config.bucket_name)
    blob = bucket.blob(config.gcs_blob_path)

    if not blob.exists():
        raise FileNotFoundError(f"File tidak ditemukan di GCS: {config.source_uri}")

    parquet_bytes = blob.download_as_bytes()
    df = pd.read_parquet(io.BytesIO(parquet_bytes))
    initial_count = len(df)

    # Identity capture HARUS dilakukan sebelum filter/dedup/transform/sort apa pun.
    # source_file_identity konsisten dipakai lewat config.gcs_blob_path (bukan literal
    # atau format string terpisah) agar setiap pemanggil menghasilkan identitas yang sama.
    df = assign_source_identity(df, source_file_identity=config.gcs_blob_path)

    df.columns = df.columns.str.lower()
    df['loaded_at'] = pd.Timestamp.now('Asia/Jakarta').tz_localize(None)

    float_columns = [
        col for col in df.columns 
        if any(keyword in col for keyword in ["amount", "fee", "surcharge", "tip", "tolls", "fare"])
    ]
    for col in float_columns:
        df[col] = df[col].astype("float64")

    integer_columns = ["vendorid", "ratecodeid", "pulocationid", "dolocationid", "passenger_count", "payment_type", "trip_type"]
    for col in integer_columns:
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    print(f"Standardisasi tipe data selesai: {len(float_columns)} kolom float, {len(integer_columns)} kolom integer.")

    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df = df[
        (df['lpep_pickup_datetime'].dt.year == config.year) & 
        (df['lpep_pickup_datetime'].dt.month == config.month)
    ]
    filtered_count = len(df)
    print(f"Filter periode selesai: dari {initial_count} baris menjadi {filtered_count} baris.")

    df = deduplicate_business_rows(df)
    dedup_count = len(df)
    print(f"Deduplikasi selesai (berbasis kolom bisnis, excl. identity/metadata): dari {filtered_count} baris menjadi {dedup_count} baris.")

    delete_existing_month_data(bq_client, config)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        # ALLOW_FIELD_ADDITION: memungkinkan load job menambah kolom baru (source_row_number,
        # source_file_identity) ke tabel staging yang sudah ada tanpa perlu ALTER TABLE manual.
        # Tetap didokumentasikan sebagai manual DDL opsional di laporan untuk kontrol eksplisit.
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="lpep_pickup_datetime"
        )
    )

    load_job = bq_client.load_table_from_dataframe(
        df, config.staging_table_id, job_config=job_config
    )
    load_job.result()

    print(f"Sukses memuat {dedup_count} baris ke tabel staging universal: {config.staging_table_id}")


def validate_staging_table(client: bigquery.Client, config: BatchPipelineConfig) -> None:
    table_id = config.staging_table_id
    
    query = f"""
        SELECT
          COUNT(*) AS total_rows,
          SUM(CASE WHEN lpep_pickup_datetime IS NULL THEN 1 ELSE 0 END) AS missing_pickup,
          SUM(CASE WHEN passenger_count < 0 THEN 1 ELSE 0 END) AS invalid_passenger_count,
          SUM(CASE WHEN trip_distance < 0 THEN 1 ELSE 0 END) AS invalid_trip_distance
        FROM `{table_id}`
        WHERE EXTRACT(YEAR FROM lpep_pickup_datetime) = {config.year}
          AND EXTRACT(MONTH FROM lpep_pickup_datetime) = {config.month}
    """

    row = next(client.query(query).result())

    if row.total_rows == 0:
        raise ValueError(f"Quality check gagal: tidak ada data ditemukan untuk {config.year}-{config.month:02d}")

    if row.missing_pickup > 0:
        raise ValueError("Quality check gagal: ditemukan nilai NULL pada kolom pickup datetime")

    if row.invalid_passenger_count > 0 or row.invalid_trip_distance > 0:
        raise ValueError("Quality check gagal: ditemukan nilai minus/negatif pada passenger_count atau trip_distance")

    print(f"Quality check staging table PASSED untuk periode {config.year}-{config.month:02d} ({row.total_rows} baris).")


def task_load_staging(year: Optional[str] = None, month: Optional[str] = None, **kwargs) -> None:
    cfg = build_config(year=int(year), month=int(month))
    load_parquet_from_gcs(cfg)


def task_validate_staging(year: Optional[str] = None, month: Optional[str] = None, **kwargs) -> None:
    cfg = build_config(year=int(year), month=int(month))
    client = bigquery.Client(project=cfg.project_id)
    validate_staging_table(client, cfg)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Load green taxi Parquet data from GCS into BigQuery staging."
    )
    parser.add_argument("--year", type=int, required=True, help="Year of the data to load")
    parser.add_argument("--month", type=int, required=True, help="Month of the data to load")
    parser.add_argument("--validate", action="store_true", help="Run staging validation after load")
    args = parser.parse_args()

    config = build_config(year=args.year, month=args.month)
    load_parquet_from_gcs(config)
    if args.validate:
        client = bigquery.Client(project=config.project_id)
        validate_staging_table(client, config)