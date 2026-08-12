import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

# Import fungsi wrapper dari modul scripts.transform
from scripts.transform import task_transform_curated, task_validate_curated

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=1),
}

with DAG(
    dag_id="aninditas_curation_dag",
    default_args=default_args,
    description="DAG untuk Mentransformasi Data dari Staging ke Curated Layer di BigQuery",
    schedule_interval=None,  # Manual Trigger
    start_date=pendulum.datetime(2026, 4, 5, tz="Asia/Jakarta"),
    catchup=False,
    tags=["curation", "bigquery", "batch"],
    # Form Parameter UI agar user wajib/bisa memilih year dan month
    params={
        "year": Param(
            default=2026,
            type="integer",
            description="Tahun data transaksi yang ingin ditransformasi (Contoh: 2024)",
            title="Tahun (Year)",
        ),
        "month": Param(
            default=4,
            type="integer",
            minimum=1,
            maximum=12,
            description="Bulan data transaksi yang ingin ditransformasi (1 - 12)",
            title="Bulan (Month)",
        ),
    },
) as dag:

    # 1. Task Transformasi Data dari Staging ke Curated
    transform_to_curated = PythonOperator(
        task_id="transform_to_curated",
        python_callable=task_transform_curated,
        op_kwargs={
            "year": "{{ params.year }}",
            "month": "{{ params.month }}",
        },
    )

    # 2. Task Validasi & Quality Check Data Curated
    validate_curated = PythonOperator(
        task_id="validate_curated",
        python_callable=task_validate_curated,
        op_kwargs={
            "year": "{{ params.year }}",
            "month": "{{ params.month }}",
        },
    )

    # Urutan Eksekusi
    transform_to_curated >> validate_curated