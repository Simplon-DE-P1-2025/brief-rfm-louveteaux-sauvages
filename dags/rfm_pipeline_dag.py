import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


CONN_ID = "DATA-DB"
RAW_TABLE = "public.raw_orders"
CLEAN_TABLE = "public.cleaned_orders"
STAGING_TABLE = "public.rfm_scores_staging"
FINAL_TABLE = "public.rfm_scores"
DIM_CLIENT_TABLE = "public.dim_client"
DIM_PRODUIT_TABLE = "public.dim_produit"
DIM_FACTURE_TABLE = "public.dim_facture"
FACT_ORDERS_TABLE = "public.fact_orders"
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


def run_ingest() -> None:
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
    df.to_sql("raw_orders", con=_engine(), schema="public", if_exists="replace", index=False, method="multi", chunksize=5000)


def run_clean() -> None:
    df = pd.read_sql(f"SELECT * FROM {RAW_TABLE}", con=_engine())

    # Logique historique de nettoyage: cast -> nulls -> retours -> invalides -> doublons
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    df = df.dropna(subset=["customer_id", "quantity", "price", "invoice_date"])
    df = df[~df["invoice"].astype(str).str.startswith("C")]
    df = df[(df["quantity"] > 0) & (df["price"] > 0)]
    df = df.drop_duplicates()
    df["total_price"] = df["quantity"] * df["price"]
    df.to_sql("cleaned_orders", con=_engine(), schema="public", if_exists="replace", index=False, method="multi", chunksize=5000)


def run_transform() -> None:
    df = pd.read_sql(f"SELECT * FROM {CLEAN_TABLE}", con=_engine())

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

    def _segment(row: pd.Series) -> str:
        r = int(row["r_score"])
        f = int(row["f_score"])
        if row["rfm_score"] == "555":
            return "Champion"
        if r >= 4 and f >= 4:
            return "Client fidele"
        if r >= 4 and f <= 2:
            return "Nouveau client"
        if r <= 2 and f >= 3:
            return "Client a risque"
        if r <= 2 and f <= 2:
            return "Client perdu"
        return "Client moyen"

    def _categorise(total: int) -> str:
        if total >= 13:
            return "VIP"
        if total >= 10:
            return "Fidele"
        if total >= 7:
            return "Regulier"
        if total >= 4:
            return "A risque"
        return "Inactif"

    rfm["segment"] = rfm.apply(_segment, axis=1)
    rfm["categorisation"] = rfm["rfm_total"].apply(_categorise)
    rfm.to_sql("rfm_scores_staging", con=_engine(), schema="public", if_exists="replace", index=False, method="multi", chunksize=5000)


def run_star_schema() -> None:
    df = pd.read_sql(f"SELECT * FROM {CLEAN_TABLE}", con=_engine())
    rfm = pd.read_sql(f"SELECT * FROM {STAGING_TABLE}", con=_engine())

    dim_client = df[["customer_id", "country"]].drop_duplicates(subset=["customer_id"]).reset_index(drop=True)
    dim_client.to_sql("dim_client", con=_engine(), schema="public", if_exists="replace", index=False, method="multi", chunksize=5000)

    dim_produit = df[["stock_code", "description"]].drop_duplicates(subset=["stock_code"]).reset_index(drop=True)
    dim_produit.to_sql("dim_produit", con=_engine(), schema="public", if_exists="replace", index=False, method="multi", chunksize=5000)

    dim_facture = df[["invoice", "customer_id", "invoice_date"]].drop_duplicates(subset=["invoice"]).reset_index(drop=True)
    dim_facture["is_retour"] = dim_facture["invoice"].astype(str).str.startswith("C")
    dim_facture.to_sql("dim_facture", con=_engine(), schema="public", if_exists="replace", index=False, method="multi", chunksize=5000)

    rfm_cols = ["customer_id", "r_score", "f_score", "m_score", "rfm_score", "rfm_total", "segment", "categorisation"]
    fact_orders = df.copy()
    fact_orders["total_line"] = fact_orders["quantity"] * fact_orders["price"]
    fact_orders = fact_orders.merge(rfm[rfm_cols], on="customer_id", how="left")
    fact_orders = fact_orders.reset_index(drop=True)
    fact_orders.to_sql("fact_orders", con=_engine(), schema="public", if_exists="replace", index=False, method="multi", chunksize=5000)


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
    clean = PythonOperator(task_id="clean", python_callable=run_clean, do_xcom_push=False)
    transform = PythonOperator(task_id="transform", python_callable=run_transform, do_xcom_push=False)
    star_schema = PythonOperator(task_id="star_schema", python_callable=run_star_schema, do_xcom_push=False)
    load = PythonOperator(task_id="load", python_callable=run_load, do_xcom_push=False)
    ingest >> clean >> transform >> star_schema >> load
