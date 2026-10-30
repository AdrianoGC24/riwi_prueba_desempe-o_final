import os
import pandas as pd

def extract_daily_sales(data_sources_dir="/opt/airflow/data_sources", staging_dir="/opt/airflow/logs/staging", **kwargs):
    """
    1. Lee el archivo diario 'data.csv'.
    2. Agrega la columna 'source_file' para saber de dónde vino.
    3. Lo guarda en la carpeta staging.
    """
    input_file = os.path.join(data_sources_dir, "data.csv")
    output_file = os.path.join(staging_dir, "raw_daily_sales.parquet")

    print(f"--> Leyendo: {input_file}")
    df = pd.read_csv(input_file, encoding="ISO-8859-1", low_memory=False)
    
    # Marcamos el origen de los datos
    df["source_file"] = "data.csv"

    # Creamos la carpeta de staging si no existe y guardamos
    os.makedirs(staging_dir, exist_ok=True)
    df.to_parquet(output_file, index=False)
    
    print(f"--> Exitoso: Se guardaron {len(df):,} filas en {output_file}")
    return output_file


def extract_historical_sales(data_sources_dir="/opt/airflow/data_sources", staging_dir="/opt/airflow/logs/staging", **kwargs):
    """
    1. Lee el archivo histórico 'online_retail.csv'.
    2. Elimina la columna innecesaria 'index'.
    3. Agrega la columna 'source_file'.
    4. Lo guarda en la carpeta staging.
    """
    input_file = os.path.join(data_sources_dir, "online_retail.csv")
    output_file = os.path.join(staging_dir, "raw_historical_sales.parquet")

    print(f"--> Leyendo: {input_file}")
    df = pd.read_csv(input_file, encoding="ISO-8859-1", low_memory=False)

    # Quitamos la columna 'index' extra que trae este archivo
    if "index" in df.columns:
        df = df.drop(columns=["index"])

    # Marcamos el origen de los datos
    df["source_file"] = "online_retail.csv"

    # Creamos la carpeta de staging si no existe y guardamos
    os.makedirs(staging_dir, exist_ok=True)
    df.to_parquet(output_file, index=False)

    print(f"--> Exitoso: Se guardaron {len(df):,} filas en {output_file}")
    return output_file


# Para probarlo directamente desde la terminal con: python dags/scripts/extract.py
if __name__ == "__main__":
    extract_daily_sales(data_sources_dir="data_sources", staging_dir="logs/staging")
    extract_historical_sales(data_sources_dir="data_sources", staging_dir="logs/staging")
