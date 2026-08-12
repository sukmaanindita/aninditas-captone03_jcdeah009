import os
from pathlib import Path
from typing import List

from dotenv import load_dotenv
from google.cloud import bigquery

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"
SQL_DIR = PROJECT_ROOT / "sql"

if ENV_FILE.exists():
    load_dotenv(dotenv_path=ENV_FILE)

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_STAGING = os.getenv("BQ_DATASET_STAGING")
DATASET_CURATED = os.getenv("BQ_DATASET_CURATED")
DATASET_MART = os.getenv("BQ_DATASET_MART")
DATASET_LOCATION = os.getenv("BQ_DATASET_LOCATION")

# Reference/master lookup table (staging) -- see create_dim_locations.sql for
# why the Data Mart is allowed to read this one table directly from staging
# instead of curated (Capstone 2 pattern: taxi zone mapping is reference
# data, not a trip/event that needs event-level curation).
STAGING_TABLE_TAXI_ZONE_LOOKUP = os.getenv("BQ_STAGING_TABLE_TAXI_ZONE_LOOKUP")

# Trip/event fact table (curated) -- the ONLY source allowed for trip/event
# data in the Data Mart.
CURATED_TABLE = os.getenv("BQ_CURATED_TABLE")

# Grain (key) columns per mart table, used both by the duplicate-key /
# null-key quality checks below and to document each table's grain in one
# place. Must match the GROUP BY columns in the corresponding sql/create_*.sql
# file.
MART_TABLE_KEY_COLUMNS = {
    "dim_locations": ["location_id"],
    "mart_daily_revenue": ["trip_date", "pickup_borough", "pickup_zone"],
    "mart_zone_performance": ["pickup_zone", "pickup_borough", "dropoff_zone", "dropoff_borough"],
    "mart_hourly_demand": ["day_of_week_num", "hour_of_day"],
}


def validate_env_vars() -> None:
    required_vars = {
        "GCP_PROJECT_ID": PROJECT_ID,
        "BQ_DATASET_STAGING": DATASET_STAGING,
        "BQ_DATASET_CURATED": DATASET_CURATED,
        "BQ_DATASET_MART": DATASET_MART,
        "BQ_DATASET_LOCATION": DATASET_LOCATION,
        "BQ_STAGING_TABLE_TAXI_ZONE_LOOKUP": STAGING_TABLE_TAXI_ZONE_LOOKUP,
        "BQ_CURATED_TABLE": CURATED_TABLE,
    }
    missing = [name for name, value in required_vars.items() if not value]
    if missing:
        raise EnvironmentError(
            f"Variabel berikut belum diatur di .env atau environment: {', '.join(missing)}"
        )


def get_bq_client() -> bigquery.Client:
    validate_env_vars()
    return bigquery.Client(project=PROJECT_ID)


def ensure_dataset(client: bigquery.Client, dataset_name: str) -> None:
    """Memastikan dataset BigQuery sudah ada sebelum tabel mart dibuat.
    Sama seperti pola ensure_dataset() di transform.py / load_lookup_zone.py."""
    dataset_id = f"{client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = DATASET_LOCATION
    client.create_dataset(dataset, exists_ok=True)


def load_sql_file(filename: str) -> str:
    sql_path = SQL_DIR / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file tidak ditemukan: {sql_path}")
    return sql_path.read_text()


def get_full_staging_zone_table_id(project_id: str) -> str:
    return f"{project_id}.{DATASET_STAGING}.{STAGING_TABLE_TAXI_ZONE_LOOKUP}"


def get_full_curated_table_id(project_id: str) -> str:
    return f"{project_id}.{DATASET_CURATED}.{CURATED_TABLE}"


def get_full_mart_table_id(project_id: str, table_name: str) -> str:
    return f"{project_id}.{DATASET_MART}.{table_name}"


def create_dim_locations():
    client = get_bq_client()
    sql = load_sql_file("create_dim_locations.sql").format(
        mart_table=get_full_mart_table_id(PROJECT_ID, "dim_locations"),
        staging_zone_table=get_full_staging_zone_table_id(PROJECT_ID),
    )
    print("Building dim_locations...")
    client.query(sql).result()
    print("dim_locations built successfully.")


def build_mart_daily_revenue():
    client = get_bq_client()
    sql = load_sql_file("create_mart_daily_revenue.sql").format(
        mart_table=get_full_mart_table_id(PROJECT_ID, "mart_daily_revenue"),
        curated_table=get_full_curated_table_id(PROJECT_ID),
        dim_locations_table=get_full_mart_table_id(PROJECT_ID, "dim_locations"),
    )
    print("Building mart_daily_revenue...")
    client.query(sql).result()
    print("mart_daily_revenue built successfully.")


def build_mart_zone_performance():
    client = get_bq_client()
    sql = load_sql_file("create_mart_zone_performance.sql").format(
        mart_table=get_full_mart_table_id(PROJECT_ID, "mart_zone_performance"),
        curated_table=get_full_curated_table_id(PROJECT_ID),
        dim_locations_table=get_full_mart_table_id(PROJECT_ID, "dim_locations"),
    )
    print("Building mart_zone_performance...")
    client.query(sql).result()
    print("mart_zone_performance built successfully.")


def build_mart_hourly_demand():
    client = get_bq_client()
    sql = load_sql_file("create_mart_hourly_demand.sql").format(
        mart_table=get_full_mart_table_id(PROJECT_ID, "mart_hourly_demand"),
        curated_table=get_full_curated_table_id(PROJECT_ID),
    )
    print("Building mart_hourly_demand...")
    client.query(sql).result()
    print("mart_hourly_demand built successfully.")


def task_merge_curated_to_mart(**kwargs):
    """
    Eksekusi pembentukan tabel Data Mart mencakup seluruh data yang ada di
    Curated Layer (trip/event fact data) + Staging Layer (taxi zone
    reference/master data untuk dim_locations, lihat create_dim_locations.sql).
    """
    client = get_bq_client()
    ensure_dataset(client, DATASET_MART)

    create_dim_locations()
    build_mart_daily_revenue()
    build_mart_zone_performance()
    build_mart_hourly_demand()


def build_null_key_check_sql(table_id: str, key_columns: List[str]) -> str:
    """Query [SAFE/read-only] untuk menghitung baris dengan NULL pada salah
    satu kolom key/grain tabel mart. Pure string-builder -- tidak melakukan
    panggilan BigQuery apa pun, sehingga bisa diuji offline."""
    if not key_columns:
        raise ValueError("key_columns tidak boleh kosong")
    conditions = " OR ".join(f"{col} IS NULL" for col in key_columns)
    return f"SELECT COUNT(*) AS null_key_count FROM `{table_id}` WHERE {conditions}"


def build_duplicate_key_check_sql(table_id: str, key_columns: List[str]) -> str:
    """Query [SAFE/read-only] untuk mendeteksi baris duplikat pada
    kombinasi kolom key/grain tabel mart. Pure string-builder -- tidak
    melakukan panggilan BigQuery apa pun, sehingga bisa diuji offline."""
    if not key_columns:
        raise ValueError("key_columns tidak boleh kosong")
    key_list = ", ".join(key_columns)
    return (
        f"SELECT {key_list}, COUNT(*) AS cnt "
        f"FROM `{table_id}` "
        f"GROUP BY {key_list} "
        f"HAVING COUNT(*) > 1"
    )


def validate_mart_table_quality(client: bigquery.Client, table_id: str, table_name: str) -> None:
    """Quality gate untuk satu tabel mart:
    1. tidak kosong (row count > 0)
    2. tidak ada NULL pada kolom key/grain
    3. tidak ada baris duplikat pada kombinasi kolom key/grain
    """
    row_cnt = list(client.query(f"SELECT COUNT(*) as row_cnt FROM `{table_id}`").result())[0].row_cnt
    if row_cnt == 0:
        raise ValueError(f"Validation Error: Table {table_name} is empty!")
    print(f"Validation Passed: Table '{table_name}' has {row_cnt:,} rows.")

    key_columns = MART_TABLE_KEY_COLUMNS[table_name]

    null_check_sql = build_null_key_check_sql(table_id, key_columns)
    null_key_count = list(client.query(null_check_sql).result())[0].null_key_count
    if null_key_count > 0:
        raise ValueError(
            f"Validation Error: Table {table_name} has {null_key_count} row(s) "
            f"with NULL in key column(s) {key_columns}!"
        )
    print(f"Validation Passed: Table '{table_name}' has no NULL key columns {key_columns}.")

    duplicate_check_sql = build_duplicate_key_check_sql(table_id, key_columns)
    duplicate_rows = list(client.query(duplicate_check_sql).result())
    if duplicate_rows:
        raise ValueError(
            f"Validation Error: Table {table_name} has {len(duplicate_rows)} "
            f"duplicate key combination(s) on {key_columns}!"
        )
    print(f"Validation Passed: Table '{table_name}' has no duplicate key {key_columns}.")


def task_validate_mart(**kwargs):
    """
    Validasi kualitas seluruh tabel Data Mart: non-empty, no NULL key,
    no duplicate key (lihat validate_mart_table_quality()).
    """
    client = get_bq_client()

    for table_name in MART_TABLE_KEY_COLUMNS:
        table_id = get_full_mart_table_id(PROJECT_ID, table_name)
        validate_mart_table_quality(client, table_id, table_name)
