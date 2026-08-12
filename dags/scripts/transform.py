import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from google.cloud import bigquery

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"
SQL_DIR = PROJECT_ROOT / "sql"

if ENV_FILE.exists():
    load_dotenv(dotenv_path=ENV_FILE)


# Murni membaca environment variables dari .env (Key disamakan 100%)
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET_STAGING = os.environ.get("BQ_DATASET_STAGING")
BQ_DATASET_CURATED = os.environ.get("BQ_DATASET_CURATED")
BQ_DATASET_LOCATION = os.environ.get("BQ_DATASET_LOCATION")

BQ_STAGING_TABLE_BATCH = os.environ.get("BQ_STAGING_TABLE_BATCH")  # <--- PERBAIKAN: Sesuai .env
BQ_STAGING_TABLE_STREAM = os.environ.get("BQ_STAGING_TABLE_STREAM")
BQ_STAGING_TABLE_TAXI_ZONE_LOOKUP = os.environ.get("BQ_STAGING_TABLE_TAXI_ZONE_LOOKUP")
BQ_CURATED_TABLE = os.environ.get("BQ_CURATED_TABLE")


def validate_env() -> None:
    required_env_vars = {
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "BQ_DATASET_STAGING": BQ_DATASET_STAGING,
        "BQ_DATASET_CURATED": BQ_DATASET_CURATED,
        "BQ_DATASET_LOCATION": BQ_DATASET_LOCATION,
        "BQ_STAGING_TABLE_BATCH": BQ_STAGING_TABLE_BATCH,
        "BQ_STAGING_TABLE_STREAM": BQ_STAGING_TABLE_STREAM,
        "BQ_STAGING_TABLE_TAXI_ZONE_LOOKUP": BQ_STAGING_TABLE_TAXI_ZONE_LOOKUP,
        "BQ_CURATED_TABLE": BQ_CURATED_TABLE,
    }

    missing_vars = [key for key, val in required_env_vars.items() if not val]
    if missing_vars:
        raise EnvironmentError(f"Variabel berikut belum didefinisikan di .env: {', '.join(missing_vars)}")


def get_full_staging_table_id(project_id: str) -> str:
    return f"{project_id}.{BQ_DATASET_STAGING}.{BQ_STAGING_TABLE_BATCH}"


def get_full_staging_stream_table_id(project_id: str) -> str:
    return f"{project_id}.{BQ_DATASET_STAGING}.{BQ_STAGING_TABLE_STREAM}"


def get_full_curated_table_id(project_id: str) -> str:
    return f"{project_id}.{BQ_DATASET_CURATED}.{BQ_CURATED_TABLE}"


def get_full_taxi_zone_table_id(project_id: str) -> str:
    return f"{project_id}.{BQ_DATASET_STAGING}.{BQ_STAGING_TABLE_TAXI_ZONE_LOOKUP}"


def load_sql_file(filename: str) -> str:
    sql_path = SQL_DIR / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file tidak ditemukan: {sql_path}")
    return sql_path.read_text()


def ensure_dataset(client: bigquery.Client, dataset_name: str) -> None:
    dataset_id = f"{client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = BQ_DATASET_LOCATION
    client.create_dataset(dataset, exists_ok=True)


def create_curated_tables(client: bigquery.Client, curated_dataset: str) -> None:
    ensure_dataset(client, curated_dataset)

    sql = load_sql_file("create_curated_tables.sql").format(
        curated_dataset=f"{client.project}.{curated_dataset}",
        curated_table=BQ_CURATED_TABLE
    )
    for statement in [stmt.strip() for stmt in sql.split(";") if stmt.strip()]:
        client.query(statement).result()


def transform_staging_to_curated(project_id: str, year: int, month: int) -> None:
    client = bigquery.Client(project=project_id)
    
    create_curated_tables(client, BQ_DATASET_CURATED)

    staging_table_id = get_full_staging_table_id(project_id)
    staging_stream_table_id = get_full_staging_stream_table_id(project_id)
    curated_table_id = get_full_curated_table_id(project_id)
    taxi_zone_table_id = get_full_taxi_zone_table_id(project_id)

    sql = load_sql_file("transform_to_curated.sql").format(
        staging_table=staging_table_id,
        staging_stream_table=staging_stream_table_id,
        curated_dataset=f"{project_id}.{BQ_DATASET_CURATED}",
        curated_table=curated_table_id,
        taxi_zone_table=taxi_zone_table_id,
        year=year,
        month=month,
    )
    
    for statement in [stmt.strip() for stmt in sql.split(";") if stmt.strip()]:
        client.query(statement).result()
    print(f"Transformasi MERGE sukses ke `{curated_table_id}` untuk periode {year}-{month:02d}.")


def validate_curated_table(client: bigquery.Client, curated_table: str, year: int, month: int) -> None:
    query = f"""
        SELECT COUNT(*) as total_rows 
        FROM `{curated_table}` 
        WHERE EXTRACT(YEAR FROM pickup_date) = {year} 
          AND EXTRACT(MONTH FROM pickup_date) = {month}
    """
    row = next(client.query(query).result())
    if row.total_rows == 0:
        raise ValueError(f"Quality check curated gagal: tidak ada data ditemukan untuk periode {year}-{month:02d}")
    print(f"Quality check curated PASSED: Ditemukan {row.total_rows} baris di {curated_table} untuk {year}-{month:02d}.")


def task_transform_curated(
    year: Optional[str] = None,
    month: Optional[str] = None,
    project_id: Optional[str] = None,
    **kwargs
) -> None:
    if year is None or month is None:
        raise ValueError("Parameter 'year' dan 'month' harus diberikan.")

    validate_env()
    clean_project = project_id if project_id and project_id != "None" else None
    resolved_project_id = clean_project or GCP_PROJECT_ID

    print(f"🚀 Menjalankan Transformasi Staging -> Curated")
    print(f"  Project ID                 : {resolved_project_id}")
    print(f"  Source Staging Batch Table  : {get_full_staging_table_id(resolved_project_id)}")
    print(f"  Source Staging Stream Table : {get_full_staging_stream_table_id(resolved_project_id)}")
    print(f"  Source Taxi Zone Table       : {get_full_taxi_zone_table_id(resolved_project_id)}")
    print(f"  Target Curated Table         : {get_full_curated_table_id(resolved_project_id)}")

    transform_staging_to_curated(
        project_id=resolved_project_id,
        year=int(year),
        month=int(month),
    )


def task_validate_curated(
    year: Optional[str] = None,
    month: Optional[str] = None,
    project_id: Optional[str] = None,
    **kwargs
) -> None:
    if year is None or month is None:
        raise ValueError("Parameter 'year' dan 'month' harus diberikan.")

    validate_env()
    clean_project = project_id if project_id and project_id != "None" else None
    resolved_project_id = clean_project or GCP_PROJECT_ID

    resolved_curated_table = get_full_curated_table_id(resolved_project_id)

    client = bigquery.Client(project=resolved_project_id)
    validate_curated_table(client, resolved_curated_table, int(year), int(month))