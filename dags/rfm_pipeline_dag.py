import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


CONN_ID = "DATA-DB"
RAW_TABLE = "raw.raw_orders"
STAGING_TABLE = "stg.rfm_scores_staging"
FINAL_TABLE = "clean.rfm_scores"
EXCEL_PATH = os.getenv("DATA_PATH", "dags/data/raw/online_retail_II.xlsx")
EXCEL_SHEET = os.getenv("EXCEL_SHEET", "Year 2010-2011")


def _engine():
    return PostgresHook(postgres_conn_id=CONN_ID).get_sqlalchemy_engine()


def _resolve_excel_path() -> str:
    configured_path = Path(EXCEL_PATH)
    candidates = []

    if configured_path.is_absolute():
        candidates.append(configured_path)
    else:
        dags_dir = Path(__file__).resolve().parent
        project_root = dags_dir.parent
        candidates.extend(
            [
                project_root / configured_path,  # ex: dags/data/raw/...
                dags_dir / configured_path,      # ex: data/raw/...
                Path("/opt/airflow") / configured_path,  # ex: container path
            ]
        )

    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    searched_paths = ", ".join(str(path) for path in candidates) or str(configured_path)
    raise FileNotFoundError(
        f"Excel file not found. DATA_PATH='{EXCEL_PATH}'. Paths checked: {searched_paths}"
    )


def _init_schemas() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            cur.execute("CREATE SCHEMA IF NOT EXISTS stg;")
            cur.execute("CREATE SCHEMA IF NOT EXISTS clean;")


def run_ingest() -> None:
    _init_schemas()
    df = pd.read_excel(_resolve_excel_path(), sheet_name=EXCEL_SHEET, dtype=str, engine="openpyxl")
    df = df.rename(
        columns={
            "Invoice": "invoice",
            "StockCode": "stock_code",
            "Description": "description",
            "Quantity": "quantity",
            "InvoiceDate": "invoice_date",
            "Price": "price",
            "Customer ID": "customer_id",
            "Country": "country",
        }
    )
    expected = ["invoice", "stock_code", "description", "quantity", "invoice_date", "price", "customer_id", "country"]
    df = df[expected]
    df.to_sql("raw_orders", con=_engine(), schema="raw", if_exists="replace", index=False, method="multi", chunksize=5000)


def run_transform() -> None:
    df = pd.read_sql(f"SELECT * FROM {RAW_TABLE}", con=_engine())
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    df = df.dropna(subset=["customer_id", "quantity", "price", "invoice_date"])
    df = df[~df["invoice"].astype(str).str.startswith("C")]
    df = df[(df["quantity"] > 0) & (df["price"] > 0)].drop_duplicates()
    df["total_price"] = df["quantity"] * df["price"]

    snapshot = df["invoice_date"].max() + pd.Timedelta(days=1)
    rfm = df.groupby("customer_id").agg(
        recency=("invoice_date", lambda x: (snapshot - x.max()).days),
        frequency=("invoice", "nunique"),
        monetary=("total_price", "sum"),
    ).reset_index()
    rfm["r_score"] = pd.qcut(rfm["recency"], q=5, labels=[5, 4, 3, 2, 1]).astype(int)
    rfm["f_score"] = pd.qcut(rfm["frequency"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5]).astype(int)
    rfm["m_score"] = pd.qcut(rfm["monetary"], q=5, labels=[1, 2, 3, 4, 5]).astype(int)
    rfm["rfm_total"] = rfm["r_score"] + rfm["f_score"] + rfm["m_score"]
    rfm["rfm_score"] = rfm["r_score"].astype(str) + rfm["f_score"].astype(str) + rfm["m_score"].astype(str)
    rfm.to_sql("rfm_scores_staging", con=_engine(), schema="stg", if_exists="replace", index=False, method="multi", chunksize=5000)


def run_load() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {FINAL_TABLE};")
            cur.execute(f"CREATE TABLE {FINAL_TABLE} AS SELECT * FROM {STAGING_TABLE};")


default_args = {"owner": "airflow", "retries": 1}


with DAG(
    dag_id="rfm_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 4),
    schedule=None,
    catchup=False,
    tags=["rfm", "pipeline"],
) as dag:
    ingest = PythonOperator(task_id="ingest", python_callable=run_ingest, do_xcom_push=False)
    transform = PythonOperator(task_id="transform", python_callable=run_transform, do_xcom_push=False)
    load = PythonOperator(task_id="load", python_callable=run_load, do_xcom_push=False)
    ingest >> transform >> load
