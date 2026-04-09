import os
import sys
from dotenv import load_dotenv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
load_dotenv(os.path.join(ROOT, ".env"))

from dags.utils.ingest_data import (
    load_excel,
    validate_columns,
    log_data_quality,
    create_raw_table,
    insert_raw_data,
)
from dags.utils.db_utils import get_connection, create_database, create_schemas

_default_data = os.path.join(ROOT, "dags", "data", "raw", "online_retail_II.xlsx")
_env_path = os.getenv("DATA_PATH")
if _env_path and os.path.isabs(_env_path):
    DATA_PATH = _env_path
else:
    DATA_PATH = _default_data

if __name__ == "__main__":

    # ── Lecture & validation ──────────────────────────────
    df = load_excel(DATA_PATH)
    df = validate_columns(df)
    log_data_quality(df)

    # ── Création base + schémas + chargement ─────────────
    create_database()
    conn = get_connection()
    create_schemas(conn)
    create_raw_table(conn)
    insert_raw_data(conn, df)
    conn.close()