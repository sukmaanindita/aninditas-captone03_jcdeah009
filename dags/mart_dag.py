from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

# 1. Pastikan sys.path di-append TERLEBIH DAHULU sebelum mengimpor dari folder scripts/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator

# 2. Impor modul dari folder scripts
from scripts.data_mart import task_merge_curated_to_mart, task_validate_mart

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="aninditas_mart_dag",
    default_args=DEFAULT_ARGS,
    description="Mart DAG: Build Data Mart tables across ALL data (Dim Locations, Daily Revenue, Zone Performance, Hourly Demand) with validation.",
    start_date=datetime(2026, 8, 8),
    schedule_interval=None,
    catchup=False,
    tags=["cp3", "batch", "mart", "bigquery", "aninditas"],
) as dag:

    # Task 1: Build & Aggregasi semua tabel Data Mart (Dimension + Mart Tables)
    build_all_mart_tables = PythonOperator(
        task_id="build_all_mart_tables",
        python_callable=task_merge_curated_to_mart,
    )

    # Task 2: Validasi Kualitas Data Mart (Row count check)
    validate_all_mart_tables = PythonOperator(
        task_id="validate_all_mart_tables",
        python_callable=task_validate_mart,
    )

    # Alur Kerja Pipeline:
    build_all_mart_tables >> validate_all_mart_tables