import os
import hashlib
import requests
import numpy as np
import pandas as pd


def _assign_categories_vectorized(descriptions: pd.Series) -> pd.Series:
    """
    Asigna una de las 5 categorías de DataMart S.A.S. (Electrónica, Hogar, Ropa, Deportes, Papelería)
    de forma vectorizada usando palabras clave sobre la descripción canónica.
    """
    desc_upper = descriptions.fillna("").str.upper()

    cond_electronica = desc_upper.str.contains(
        r"LIGHT|LAMP|CLOCK|RADIO|TORCH|USB|CAMERA|PHONE|CALCULATOR|BATTERY|LED|ALARM|PLUG|BULB",
        regex=True
    )
    cond_papeleria = desc_upper.str.contains(
        r"PAPER|PEN|PENCIL|NOTEBOOK|CARD|STICKER|BOOK|WRAP|TAPE|RIBBON|DIARY|ERASER|RULER|STAMP|ENVELOPE|POSTAGE",
        regex=True
    )
    cond_ropa = desc_upper.str.contains(
        r"BAG|SHIRT|DRESS|SOCK|HAT|SCARF|APRON|PURSE|WALLET|UMBRELLA|SLIPPER|COAT|GLOVE|JEWEL|NECKLACE|BRACELET|RING",
        regex=True
    )
    cond_deportes = desc_upper.str.contains(
        r"BALL|GAME|BIKE|CYCLE|SPORT|CAMPING|PICNIC|BOTTLE|SKIPPING|DART|KITE|PUZZLE|TOY|OUTDOOR|BEACH",
        regex=True
    )

    # Si no cae en las 4 anteriores, por naturaleza del catálogo pertenece a 'Hogar'
    return np.select(
        [cond_electronica, cond_papeleria, cond_ropa, cond_deportes],
        ["Electrónica", "Papelería", "Ropa", "Deportes"],
        default="Hogar"
    )


def _fetch_categories_from_api(stock_codes: list, api_url: str) -> dict:
    """Plus opcional (Sección 4.3): Consulta la API interna de catálogo si está activa."""
    catalog_map = {}
    try:
        response = requests.get(f"{api_url}/products", params={"limit": 10000}, timeout=5)
        if response.status_code == 200:
            items = response.json().get("items", [])
            for item in items:
                catalog_map[item["stock_code"]] = item
            print(f"--> [API] Catálogo enriquecido desde API con {len(catalog_map)} productos.")
    except Exception as e:
        print(f"--> [API Info] API de catálogo no activa ({e}). Usando estrategia heurística de respaldo.")
    return catalog_map


def _make_hash(*series_list):
    """Genera hashes MD5 vectorizados a partir de una lista de series."""
    combined = series_list[0].astype(str)
    for s in series_list[1:]:
        combined = combined + "|" + s.astype(str)
    return [hashlib.md5(val.encode("utf-8")).hexdigest() for val in combined]


def transform_and_validate(staging_dir="/opt/airflow/logs/staging", use_catalog_api="false", api_url="http://fastapi-catalog:8000", **kwargs):
    # 1. Cargar datasets crudos desde la zona de staging
    path_daily = os.path.join(staging_dir, "raw_daily_sales.parquet")
    path_historical = os.path.join(staging_dir, "raw_historical_sales.parquet")

    if not os.path.exists(path_daily) or not os.path.exists(path_historical):
        raise FileNotFoundError(f"Archivos no encontrados en staging: {staging_dir}")

    df_daily = pd.read_parquet(path_daily)
    df_historical = pd.read_parquet(path_historical)

    # 2. Validación de columnas
    cols_daily = list(df_daily.columns)
    cols_historical = list(df_historical.columns)

    if cols_daily != cols_historical:
        missing_in_hist = set(cols_daily) - set(cols_historical)
        missing_in_daily = set(cols_historical) - set(cols_daily)
        if missing_in_hist or missing_in_daily:
            raise ValueError(f"Inconsistencia en columnas. Histórico: {missing_in_hist}, Diario: {missing_in_daily}")
        df_historical = df_historical[cols_daily]

    # 3. Concatenación vertical y desduplicación exacta
    df_combined = pd.concat([df_daily, df_historical], ignore_index=True)
    df_combined = df_combined.drop_duplicates(
        subset=["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
    ).copy()

    # 4. Limpieza básica de strings y remoción de sufijo '.0'
    text_columns = ["InvoiceNo", "StockCode", "Description", "Country"]
    for col in text_columns:
        if col in df_combined.columns:
            df_combined[col] = df_combined[col].fillna("").astype(str).str.strip().str.upper()
            df_combined[col] = df_combined[col].replace({"": None, "NAN": None, "NONE": None})

    if "CustomerID" in df_combined.columns:
        cust_numeric = pd.to_numeric(df_combined["CustomerID"], errors="coerce").astype("Int64")
        df_combined["CustomerID"] = cust_numeric.astype(str).replace({"<NA>": None, "nan": None})

    # Guardamos copia cruda para la tabla de auditoría
    raw_df = df_combined.copy()

    # 5. Detección y Auditoría de Rechazos
    unit_price_num = pd.to_numeric(df_combined["UnitPrice"], errors="coerce")
    quantity_num = pd.to_numeric(df_combined["Quantity"], errors="coerce")
    invoice_date_dt = pd.to_datetime(df_combined["InvoiceDate"], errors="coerce", utc=True)

    rejection_reasons = pd.Series(None, index=df_combined.index, dtype=object)

    rejection_reasons.loc[df_combined["InvoiceNo"].isnull()] = "MISSING_INVOICE_NO"
    rejection_reasons.loc[rejection_reasons.isnull() & df_combined["StockCode"].isnull()] = "MISSING_STOCK_CODE"
    rejection_reasons.loc[rejection_reasons.isnull() & quantity_num.isnull()] = "MISSING_OR_INVALID_QUANTITY"
    rejection_reasons.loc[rejection_reasons.isnull() & invoice_date_dt.isnull()] = "MISSING_OR_INVALID_INVOICE_DATE"
    rejection_reasons.loc[rejection_reasons.isnull() & (unit_price_num.isnull() | (unit_price_num <= 0))] = "PRICE_ZERO_OR_NEGATIVE"

    mask_rejected = rejection_reasons.notnull()

    # 5.1 Construir DataFrame de auditoría de rechazos
    df_rejected = raw_df[mask_rejected].copy()
    if not df_rejected.empty:
        df_rejected["rejection_reason"] = rejection_reasons[mask_rejected]

        # Generar hash único de idempotencia para cada registro rechazado
        df_rejected["reject_hash"] = _make_hash(
            df_rejected["InvoiceNo"].fillna(""),
            df_rejected["StockCode"].fillna(""),
            df_rejected["InvoiceDate"].fillna(""),
            df_rejected["Quantity"].fillna(""),
            df_rejected["UnitPrice"].fillna(""),
            df_rejected["rejection_reason"]
        )

        df_rejected = df_rejected.rename(columns={
            "InvoiceNo": "raw_invoice_no",
            "StockCode": "raw_stock_code",
            "Description": "raw_description",
            "Quantity": "raw_quantity",
            "UnitPrice": "raw_unit_price",
            "InvoiceDate": "raw_invoice_date",
            "CustomerID": "raw_customer_id",
            "Country": "raw_country",
        })[[
            "reject_hash", "raw_invoice_no", "raw_stock_code", "raw_description",
            "raw_quantity", "raw_unit_price", "raw_invoice_date", "raw_customer_id",
            "raw_country", "rejection_reason", "source_file"
        ]]

    # 5.2 Filtrar registros válidos
    df_valid = df_combined[~mask_rejected].copy()
    df_valid["Quantity"] = quantity_num[~mask_rejected].astype(int)
    df_valid["UnitPrice"] = unit_price_num[~mask_rejected].round(4)
    df_valid["InvoiceDate_UTC"] = invoice_date_dt[~mask_rejected]

    # Regla 3: Clasificación transaccional (SALE vs RETURN)
    df_valid["transaction_type"] = np.where(df_valid["Quantity"] > 0, "SALE", "RETURN")
    df_valid["gross_amount"] = (df_valid["Quantity"] * df_valid["UnitPrice"]).round(4)

    # Regla 4: Clientes no identificados como 'UNIDENTIFIED'
    df_valid["CustomerID"] = df_valid["CustomerID"].fillna("UNIDENTIFIED")

    # Clave de fecha (YYYYMMDD)
    df_valid["date_key"] = df_valid["InvoiceDate_UTC"].dt.strftime("%Y%m%d").astype(int)

    # Hash único de idempotencia por transacción
    df_valid["transaction_hash"] = _make_hash(
        df_valid["InvoiceNo"],
        df_valid["StockCode"],
        df_valid["InvoiceDate_UTC"],
        df_valid["Quantity"],
        df_valid["UnitPrice"],
        df_valid["CustomerID"]
    )

    # 6. Construir Dimensión de Fechas (dim_date)
    unique_dates = df_valid["InvoiceDate_UTC"].dt.date.unique()
    df_dim_date = pd.DataFrame({"full_date": pd.to_datetime(unique_dates)})
    df_dim_date["date_key"] = df_dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    df_dim_date["full_date"] = df_dim_date["full_date"].dt.date
    df_dim_date["year"] = pd.to_datetime(df_dim_date["full_date"]).dt.year
    df_dim_date["quarter"] = pd.to_datetime(df_dim_date["full_date"]).dt.quarter
    df_dim_date["month"] = pd.to_datetime(df_dim_date["full_date"]).dt.month
    df_dim_date["month_name"] = pd.to_datetime(df_dim_date["full_date"]).dt.strftime("%B")
    df_dim_date["day"] = pd.to_datetime(df_dim_date["full_date"]).dt.day
    df_dim_date["day_of_week"] = pd.to_datetime(df_dim_date["full_date"]).dt.dayofweek + 1
    df_dim_date["is_weekend"] = df_dim_date["day_of_week"].isin([6, 7])
    df_dim_date = df_dim_date[[
        "date_key", "full_date", "year", "quarter", "month", "month_name", "day", "day_of_week", "is_weekend"
    ]].sort_values("date_key").reset_index(drop=True)

    # 7. Construir Dimensión de Clientes (dim_customers)
    cust_country = (
        df_valid.groupby(["CustomerID", "Country"])
        .size()
        .reset_index(name="counts")
        .sort_values(["CustomerID", "counts"], ascending=[True, False])
        .drop_duplicates(subset=["CustomerID"])
    )
    df_dim_customers = pd.DataFrame({
        "customer_id": cust_country["CustomerID"],
        "is_identified": cust_country["CustomerID"] != "UNIDENTIFIED",
        "primary_country": cust_country["Country"],
    }).reset_index(drop=True)

    # 8. Construir Dimensión de Productos (dim_products)
    desc_clean = df_valid[df_valid["Description"].notnull()]
    desc_mode = (
        desc_clean.groupby(["StockCode", "Description"])
        .size()
        .reset_index(name="freq")
        .sort_values(["StockCode", "freq"], ascending=[True, False])
        .drop_duplicates(subset=["StockCode"])
    )

    desc_variations = (
        desc_clean.groupby("StockCode")["Description"]
        .nunique()
        .reset_index(name="raw_variation_count")
    )

    all_products = pd.DataFrame({"stock_code": df_valid["StockCode"].unique()})
    df_dim_products = all_products.merge(
        desc_mode[["StockCode", "Description"]].rename(columns={"Description": "canonical_description"}),
        left_on="stock_code", right_on="StockCode", how="left"
    ).merge(
        desc_variations.rename(columns={"StockCode": "stock_code_var"}),
        left_on="stock_code", right_on="stock_code_var", how="left"
    )

    df_dim_products["canonical_description"] = df_dim_products["canonical_description"].fillna("UNKNOWN PRODUCT")
    df_dim_products["raw_variation_count"] = df_dim_products["raw_variation_count"].fillna(1).astype(int)

    # Enriquecimiento híbrido: API (si está activa) o vectorización heurística
    api_catalog = {}
    if str(use_catalog_api).lower() == "true":
        api_catalog = _fetch_categories_from_api(df_dim_products["stock_code"].tolist(), api_url)

    if api_catalog:
        df_dim_products["category"] = df_dim_products["stock_code"].apply(
            lambda sc: api_catalog.get(sc, {}).get("category")
        )
        # Fallback para los que no estaban en la API
        mask_missing = df_dim_products["category"].isnull()
        if mask_missing.any():
            df_dim_products.loc[mask_missing, "category"] = _assign_categories_vectorized(
                df_dim_products.loc[mask_missing, "canonical_description"]
            )
    else:
        df_dim_products["category"] = _assign_categories_vectorized(df_dim_products["canonical_description"])

    df_dim_products["supplier_country"] = "United Kingdom"
    df_dim_products["is_active"] = True

    df_dim_products = df_dim_products[[
        "stock_code", "canonical_description", "category", "supplier_country", "is_active", "raw_variation_count"
    ]].reset_index(drop=True)

    # 9. Construir Hechos Transaccionales (fact_transactions)
    df_fact_transactions = df_valid[[
        "transaction_hash", "InvoiceNo", "StockCode", "CustomerID", "date_key",
        "InvoiceDate_UTC", "Country", "transaction_type", "Quantity", "UnitPrice",
        "gross_amount", "source_file"
    ]].rename(columns={
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "CustomerID": "customer_id",
        "InvoiceDate_UTC": "invoice_timestamp_utc",
        "Country": "country",
        "Quantity": "quantity",
        "UnitPrice": "unit_price",
    }).reset_index(drop=True)

    # 10. Construir Agregado Diario por Producto (fact_daily_product_metrics)
    df_valid["sale_units"] = np.where(df_valid["Quantity"] > 0, df_valid["Quantity"], 0)
    df_valid["return_units"] = np.where(df_valid["Quantity"] < 0, np.abs(df_valid["Quantity"]), 0)
    df_valid["sale_gross"] = np.where(df_valid["Quantity"] > 0, df_valid["gross_amount"], 0.0)
    df_valid["return_gross"] = np.where(df_valid["Quantity"] < 0, np.abs(df_valid["gross_amount"]), 0.0)
    df_valid["is_sale_tx"] = np.where(df_valid["transaction_type"] == "SALE", 1, 0)
    df_valid["is_return_tx"] = np.where(df_valid["transaction_type"] == "RETURN", 1, 0)

    daily_metrics = df_valid.groupby(["date_key", "StockCode"]).agg(
        sales_count=("is_sale_tx", "sum"),
        returns_count=("is_return_tx", "sum"),
        units_sold=("sale_units", "sum"),
        units_returned=("return_units", "sum"),
        gross_revenue=("sale_gross", "sum"),
        returns_amount=("return_gross", "sum"),
    ).reset_index()

    daily_metrics["net_revenue"] = (daily_metrics["gross_revenue"] - daily_metrics["returns_amount"]).round(4)
    daily_metrics["gross_revenue"] = daily_metrics["gross_revenue"].round(4)
    daily_metrics["returns_amount"] = daily_metrics["returns_amount"].round(4)
    daily_metrics = daily_metrics.rename(columns={"StockCode": "stock_code"})

    df_fact_daily_metrics = daily_metrics[[
        "date_key", "stock_code", "sales_count", "returns_count", "units_sold",
        "units_returned", "gross_revenue", "returns_amount", "net_revenue"
    ]]

    # 11. Persistir todas las tablas en staging como Parquet
    os.makedirs(staging_dir, exist_ok=True)
    df_dim_date.to_parquet(os.path.join(staging_dir, "dim_date.parquet"), index=False)
    df_dim_products.to_parquet(os.path.join(staging_dir, "dim_products.parquet"), index=False)
    df_dim_customers.to_parquet(os.path.join(staging_dir, "dim_customers.parquet"), index=False)
    df_fact_transactions.to_parquet(os.path.join(staging_dir, "fact_transactions.parquet"), index=False)
    df_fact_daily_metrics.to_parquet(os.path.join(staging_dir, "fact_daily_product_metrics.parquet"), index=False)
    df_rejected.to_parquet(os.path.join(staging_dir, "audit_rejected_records.parquet"), index=False)

    print("--> Transformación finalizada con éxito. Modelos generados:")
    print(f"    - dim_date: {len(df_dim_date):,} filas")
    print(f"    - dim_products: {len(df_dim_products):,} filas")
    print(f"    - dim_customers: {len(df_dim_customers):,} filas")
    print(f"    - fact_transactions: {len(df_fact_transactions):,} transacciones")
    print(f"    - fact_daily_product_metrics: {len(df_fact_daily_metrics):,} agregados diarios")
    print(f"    - audit_rejected_records: {len(df_rejected):,} registros rechazados")

    return staging_dir

