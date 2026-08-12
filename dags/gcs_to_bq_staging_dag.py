from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

# 1. Pastikan sys.path di-append TERLEBIH DAHULU sebelum mengimpor dari folder batch/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

# 2. Impor modul dari package scripts yang ada dalam folder dags/scripts
from scripts.download_to_gcs import task_load_to_gcs
from scripts.load_lookup_zone import task_load_taxi_zone
from scripts.load_staging import task_load_staging, task_validate_staging

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="aninditas_batch_pipeline_dag",
    default_args=DEFAULT_ARGS,
    description="Staging DAG: load data from GCS & Zone Lookup to BigQuery staging with technical validation.",
    start_date=datetime(2026, 8, 8),
    schedule_interval=None,
    catchup=False,
    tags=["cp3", "batch", "staging", "bigquery", "aninditas"],
    params={
        "year": Param("2026", type="string", description="Tahun data"),
        "month": Param("04", type="string", description="Bulan data (04 untuk April, 05 untuk Mei)"),
    },
) as dag:

    # Task 1A: Ingest Raw Parquet dari TLC ke GCS
    ingest_raw_gcs = PythonOperator(
        task_id="ingest_raw_gcs",
        python_callable=task_load_to_gcs,
        op_kwargs={
            "year": "{{ params.year }}",
            "month": "{{ params.month }}",
        },
    )

    # Task 1B: Ingest Data Master Taxi Zone Lookup CSV langsung ke BQ Staging
    load_taxi_zone = PythonOperator(
        task_id="load_taxi_zone",
        python_callable=task_load_taxi_zone,
    )

    # Task 2: Load & Upsert Staging Data Green Taxi ke BQ Staging
    load_task = PythonOperator(
        task_id="load_staging",
        python_callable=task_load_staging,
        op_kwargs={
            "year": "{{ params.year }}",
            "month": "{{ params.month }}",
        },
    )

    # Task 3: Technical Validation Staging Data
    validate_staging = PythonOperator(
        task_id="validate_staging",
        python_callable=task_validate_staging,
        op_kwargs={
            "year": "{{ params.year }}",
            "month": "{{ params.month }}",
        },
    )

    # Alur Kerja Pipeline:
    # Task ingest_raw_gcs dan load_taxi_zone berjalan sejajar (paralel).
    # Setelah ingest_raw_gcs selesai, load_task dijalankan lalu dilanjutkan ke validate_staging.
    [ingest_raw_gcs, load_taxi_zone] >> load_task >> validate_staging