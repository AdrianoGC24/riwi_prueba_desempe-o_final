import os
import pandas as pd
import numpy as np

def print_separator(title, char="="):
    print(f"\n{char*80}")
    print(f" {title.upper()} ".center(80, char))
    print(f"{char*80}\n")

def analyze_dataset(file_path, dataset_name):
    print_separator(f"ANÁLISIS EXPLORATORIO: {dataset_name}")
    
    if not os.path.exists(file_path):
        print(f"ERROR: Archivo no encontrado en la ruta {file_path}")
        return None

    # Lectura del archivo con codificación ISO-8859-1 (común en datasets de retail/e-commerce)
    try:
        df = pd.read_csv(file_path, encoding='ISO-8859-1', low_memory=False)
    except Exception as e:
        print(f"Error al leer con ISO-8859-1: {e}. Intentando utf-8...")
        df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)

    total_rows = len(df)
    total_cols = len(df.columns)
    print(f"1. ESTRUCTURA GENERAL:")
    print(f"   - Total de filas: {total_rows:,}")
    print(f"   - Total de columnas: {total_cols}")
    print(f"   - Columnas detectadas: {list(df.columns)}\n")

    print(f"2. TIPOS DE DATOS Y VALORES NULOS:")
    null_summary = pd.DataFrame({
        'Tipo de Dato': df.dtypes,
        'Nulos': df.isnull().sum(),
        '% Nulos': (df.isnull().sum() / total_rows * 100).round(2)
    })
    print(null_summary.to_string())
    print()

    print(f"3. REGISTROS DUPLICADOS:")
    dup_count = df.duplicated().sum()
    print(f"   - Filas exactamente duplicadas: {dup_count:,} ({dup_count/total_rows*100:.2f}%)\n")

    # Análisis de campos clave si existen
    print("4. ANÁLISIS DE REGLAS DE NEGOCIO Y CALIDAD DE DATOS:")
    
    # Cantidad (Quantity)
    if 'Quantity' in df.columns:
        neg_qty = (df['Quantity'] < 0).sum()
        zero_qty = (df['Quantity'] == 0).sum()
        pos_qty = (df['Quantity'] > 0).sum()
        print(f"   [Quantity]")
        print(f"     * Rango: Mínimo = {df['Quantity'].min()}, Máximo = {df['Quantity'].max()}")
        print(f"     * Positivas (Ventas normales): {pos_qty:,} ({pos_qty/total_rows*100:.2f}%)")
        print(f"     * Negativas (Devoluciones/Ajustes): {neg_qty:,} ({neg_qty/total_rows*100:.2f}%)")
        print(f"     * Cero: {zero_qty:,}")
    
    # Precio Unitario (UnitPrice / Price)
    price_col = 'UnitPrice' if 'UnitPrice' in df.columns else ('Price' if 'Price' in df.columns else None)
    if price_col:
        neg_price = (df[price_col] < 0).sum()
        zero_price = (df[price_col] == 0).sum()
        pos_price = (df[price_col] > 0).sum()
        print(f"   [{price_col}]")
        print(f"     * Rango: Mínimo = {df[price_col].min()}, Máximo = {df[price_col].max()}")
        print(f"     * Mayores a 0 (Válidos): {pos_price:,} ({pos_price/total_rows*100:.2f}%)")
        print(f"     * Cero: {zero_price:,} ({zero_price/total_rows*100:.2f}%)")
        print(f"     * Negativos (Anomalías/Ajustes): {neg_price:,} ({neg_price/total_rows*100:.2f}%)")

    # Clientes (CustomerID)
    cust_col = 'CustomerID' if 'CustomerID' in df.columns else ('Customer ID' if 'Customer ID' in df.columns else None)
    if cust_col:
        missing_cust = df[cust_col].isnull().sum()
        unique_cust = df[cust_col].nunique()
        print(f"   [{cust_col}]")
        print(f"     * Clientes únicos identificados: {unique_cust:,}")
        print(f"     * Registros sin Customer ID: {missing_cust:,} ({missing_cust/total_rows*100:.2f}%)")

    # Fechas (InvoiceDate)
    date_col = 'InvoiceDate' if 'InvoiceDate' in df.columns else None
    if date_col:
        try:
            parsed_dates = pd.to_datetime(df[date_col], errors='coerce')
            print(f"   [{date_col}]")
            print(f"     * Fecha mínima: {parsed_dates.min()}")
            print(f"     * Fecha máxima: {parsed_dates.max()}")
            print(f"     * Errores de parseo de fecha: {parsed_dates.isnull().sum():,}")
        except Exception as e:
            print(f"   [{date_col}] No se pudo parsear fecha directamente: {e}")

    # Códigos de Producto y Descripciones
    stock_col = 'StockCode' if 'StockCode' in df.columns else None
    desc_col = 'Description' if 'Description' in df.columns else None
    if stock_col:
        print(f"   [{stock_col}]")
        unique_stock = df[stock_col].nunique()
        print(f"     * Códigos únicos de producto: {unique_stock:,}")
        
        # Códigos alfanuméricos vs no convencionales
        sample_codes = df[stock_col].dropna().astype(str).str.strip().unique()
        special_codes = [c for c in sample_codes if not any(char.isdigit() for char in c)]
        print(f"     * Ejemplos de códigos especiales (sin dígitos): {special_codes[:10]}")

    if stock_col and desc_col:
        # StockCodes con múltiples descripciones
        stock_desc_counts = df.dropna(subset=[stock_col, desc_col]).groupby(stock_col)[desc_col].nunique()
        multi_desc = (stock_desc_counts > 1).sum()
        print(f"   [Inconsistencia de Descripciones]")
        print(f"     * Productos con más de una descripción asociada: {multi_desc:,}")

    # Países
    country_col = 'Country' if 'Country' in df.columns else None
    if country_col:
        print(f"   [{country_col}]")
        print(f"     * Países únicos: {df[country_col].nunique()}")
        print(f"     * Top 5 países por transacciones:\n{df[country_col].value_counts().head(5).to_string()}")

    print()
    return df

def compare_datasets(df1, df2, name1="data.csv", name2="online_retail.csv"):
    print_separator("COMPARACIÓN Y TRASLAPE ENTRE AMBAS FUENTES")
    if df1 is None or df2 is None:
        print("No se pueden comparar los datasets porque uno no cargó correctamente.")
        return

    # Normalizar columnas para comparar
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)
    
    print("1. COMPARACIÓN DE COLUMNAS:")
    print(f"   - Columnas comunes: {cols1.intersection(cols2)}")
    print(f"   - Columnas solo en {name1}: {cols1 - cols2}")
    print(f"   - Columnas solo en {name2}: {cols2 - cols1}")
    print()

    # Rango temporal
    if 'InvoiceDate' in df1.columns and 'InvoiceDate' in df2.columns:
        dates1 = pd.to_datetime(df1['InvoiceDate'], errors='coerce')
        dates2 = pd.to_datetime(df2['InvoiceDate'], errors='coerce')
        print("2. COMPARACIÓN DE RANGO TEMPORAL:")
        print(f"   - {name1}: desde {dates1.min()} hasta {dates1.max()}")
        print(f"   - {name2}: desde {dates2.min()} hasta {dates2.max()}")
        print()

    # Facturas compartidas
    if 'InvoiceNo' in df1.columns and 'InvoiceNo' in df2.columns:
        invoices1 = set(df1['InvoiceNo'].dropna().astype(str))
        invoices2 = set(df2['InvoiceNo'].dropna().astype(str))
        common_invoices = invoices1.intersection(invoices2)
        print("3. ANÁLISIS DE FACTURAS (InvoiceNo):")
        print(f"   - Facturas en {name1}: {len(invoices1):,}")
        print(f"   - Facturas en {name2}: {len(invoices2):,}")
        print(f"   - Facturas compartidas (solapamiento): {len(common_invoices):,}")
        print()

def main():
    print("Iniciando Análisis Exploratorio de Datos (EDA) para DataMart S.A.S...")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data_sources")
    
    path_data = os.path.join(data_dir, "data.csv")
    path_retail = os.path.join(data_dir, "online_retail.csv")
    
    df_data = analyze_dataset(path_data, "data.csv")
    df_retail = analyze_dataset(path_retail, "online_retail.csv")
    
    compare_datasets(df_data, df_retail, "data.csv", "online_retail.csv")
    
    print_separator("FIN DEL ANÁLISIS EXPLORATORIO")

if __name__ == "__main__":
    main()

