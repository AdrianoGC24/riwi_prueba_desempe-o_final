import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd


def _get_db_connection(conn_id="postgres_dw"):
    """
    Obtiene la conexión a PostgreSQL:
    1. A través de Airflow Connection ('postgres_dw') si corre dentro del DAG.
    2. Fallback a variables de entorno si se corre en local/standalone.
    """
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(conn_id)
        return psycopg2.connect(
            host=conn.host,
            port=conn.port or 5432,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema
        )
    except Exception:
        # Modo autónomo / prueba local fuera de Airflow
        return psycopg2.connect(
            host=os.getenv("DW_HOST", "localhost"),
            port=int(os.getenv("DW_EXTERNAL_PORT", 5433)),
            user=os.getenv("DW_USER", "dw_admin"),
            password=os.getenv("DW_PASSWORD", "dw_secure_password_2026"),
            dbname=os.getenv("DW_DB", "datamart_dw")
        )


def _load_batch_idempotent(cursor, table_name, columns, data_tuples, conflict_clause, page_size=20000):
    """
    Inserta datos masivos en PostgreSQL a alta velocidad con execute_values e idempotencia.
    """
    if not data_tuples:
        return 0

    cols_str = ", ".join(columns)
    query = f"""
        INSERT INTO {table_name} ({cols_str})
        VALUES %s
        {conflict_clause}
    """
    execute_values(cursor, query, data_tuples, page_size=page_size)
    return len(data_tuples)


def load_star_schema_to_dw(staging_dir="/opt/airflow/logs/staging", conn_id="postgres_dw", **kwargs):
    print("--> Conectando con PostgreSQL Data Warehouse...")
    conn = _get_db_connection(conn_id)
    cursor = conn.cursor()

    try:
        # 1. CARGA DE DIMENSIONES (Primero para satisfacer Foreign Keys)
        # 1.1 dim_date
        path_date = os.path.join(staging_dir, "dim_date.parquet")
        if os.path.exists(path_date):
            df_date = pd.read_parquet(path_date)
            cols = ["date_key", "full_date", "year", "quarter", "month", "month_name", "day", "day_of_week", "is_weekend"]
            tuples = list(df_date[cols].itertuples(index=False, name=None))
            _load_batch_idempotent(
                cursor, "dim_date", cols, tuples,
                conflict_clause="ON CONFLICT (date_key) DO NOTHING"
            )

        # 1.2 dim_customers
        path_cust = os.path.join(staging_dir, "dim_customers.parquet")
        if os.path.exists(path_cust):
            df_cust = pd.read_parquet(path_cust)
            cols = ["customer_id", "is_identified", "primary_country"]
            tuples = list(df_cust[cols].itertuples(index=False, name=None))
            _load_batch_idempotent(
                cursor, "dim_customers", cols, tuples,
                conflict_clause="""
                    ON CONFLICT (customer_id) DO UPDATE SET
                        is_identified = EXCLUDED.is_identified,
                        primary_country = EXCLUDED.primary_country,
                        updated_at = CURRENT_TIMESTAMP
                """
            )

        # 1.3 dim_products
        path_prod = os.path.join(staging_dir, "dim_products.parquet")
        if os.path.exists(path_prod):
            df_prod = pd.read_parquet(path_prod)
            cols = ["stock_code", "canonical_description", "category", "supplier_country", "is_active", "raw_variation_count"]
            tuples = list(df_prod[cols].itertuples(index=False, name=None))
            _load_batch_idempotent(
                cursor, "dim_products", cols, tuples,
                conflict_clause="""
                    ON CONFLICT (stock_code) DO UPDATE SET
                        canonical_description = EXCLUDED.canonical_description,
                        category = EXCLUDED.category,
                        raw_variation_count = EXCLUDED.raw_variation_count,
                        updated_at = CURRENT_TIMESTAMP
                """
            )

        conn.commit()

        # 2. CARGA DE HECHOS Y AUDITORÍA
        # 2.1 audit_rejected_records
        path_rejected = os.path.join(staging_dir, "audit_rejected_records.parquet")
        if os.path.exists(path_rejected):
            df_rejected = pd.read_parquet(path_rejected)
            cols = [
                "reject_hash", "raw_invoice_no", "raw_stock_code", "raw_description",
                "raw_quantity", "raw_unit_price", "raw_invoice_date", "raw_customer_id",
                "raw_country", "rejection_reason", "source_file"
            ]
            tuples = list(df_rejected[cols].itertuples(index=False, name=None))
            _load_batch_idempotent(
                cursor, "audit_rejected_records", cols, tuples,
                conflict_clause="ON CONFLICT (reject_hash) DO NOTHING"
            )

        # 2.2 fact_daily_product_metrics
        path_metrics = os.path.join(staging_dir, "fact_daily_product_metrics.parquet")
        if os.path.exists(path_metrics):
            df_metrics = pd.read_parquet(path_metrics)
            cols = [
                "date_key", "stock_code", "sales_count", "returns_count", "units_sold",
                "units_returned", "gross_revenue", "returns_amount", "net_revenue"
            ]
            tuples = list(df_metrics[cols].itertuples(index=False, name=None))
            _load_batch_idempotent(
                cursor, "fact_daily_product_metrics", cols, tuples,
                conflict_clause="""
                    ON CONFLICT (date_key, stock_code) DO UPDATE SET
                        sales_count = EXCLUDED.sales_count,
                        returns_count = EXCLUDED.returns_count,
                        units_sold = EXCLUDED.units_sold,
                        units_returned = EXCLUDED.units_returned,
                        gross_revenue = EXCLUDED.gross_revenue,
                        returns_amount = EXCLUDED.returns_amount,
                        net_revenue = EXCLUDED.net_revenue,
                        updated_at = CURRENT_TIMESTAMP
                """
            )

        # 2.3 fact_transactions
        path_trans = os.path.join(staging_dir, "fact_transactions.parquet")
        if os.path.exists(path_trans):
            df_trans = pd.read_parquet(path_trans)
            cols = [
                "transaction_hash", "invoice_no", "stock_code", "customer_id", "date_key",
                "invoice_timestamp_utc", "country", "transaction_type", "quantity", "unit_price",
                "gross_amount", "source_file"
            ]
            df_trans["invoice_timestamp_utc"] = df_trans["invoice_timestamp_utc"].astype(str)
            tuples = list(df_trans[cols].itertuples(index=False, name=None))
            _load_batch_idempotent(
                cursor, "fact_transactions", cols, tuples,
                conflict_clause="ON CONFLICT (transaction_hash) DO NOTHING"
            )

        conn.commit()

        print("--> Carga finalizada con éxito en PostgreSQL Data Warehouse.")
        print(f"    - dim_date: {len(df_date):,} registros procesados")
        print(f"    - dim_products: {len(df_prod):,} registros procesados")
        print(f"    - dim_customers: {len(df_cust):,} registros procesados")
        print(f"    - fact_transactions: {len(df_trans):,} registros procesados")
        print(f"    - fact_daily_product_metrics: {len(df_metrics):,} registros procesados")
        print(f"    - audit_rejected_records: {len(df_rejected):,} registros procesados")

    except Exception as e:
        conn.rollback()
        print(f"--> ERROR en la carga a PostgreSQL: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

