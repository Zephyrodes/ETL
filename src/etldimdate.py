import pandas as pd
from datetime import datetime, timedelta
import holidays
import boto3
import io
import pyarrow as pa
import pyarrow.parquet as pq

# ===================== CONFIGURACIÓN =====================
bucket = "etlsakila"                   # tu bucket
prefix = "dim_date/"                   # carpeta donde guardarás la tabla
key = f"{prefix}dim_date.parquet"      # archivo final

s3 = boto3.client("s3")
us_holidays = holidays.US()

# ===================== 1. Leer archivo existente (si existe) =====================
try:
    obj = s3.get_object(Bucket=bucket, Key=key)
    existing_df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    last_date = pd.to_datetime(existing_df["rental_date"]).max()
    print(f"Última fecha cargada: {last_date}")
except Exception:
    print("No existe dim_date aún. Se iniciará desde 2005-01-01")
    existing_df = pd.DataFrame()
    last_date = datetime(2005, 1, 1) - timedelta(days=1)

# ===================== 2. Generar nuevas fechas =====================
today = datetime.today()
end_date = today + timedelta(days=365)  # hasta 1 año adelante

new_dates = pd.date_range(start=last_date + timedelta(days=1), end=end_date)

if new_dates.empty:
    print("No hay nuevas fechas para agregar.")
else:
    print(f"Generando fechas desde {new_dates.min()} hasta {new_dates.max()}")
    

    # ===================== 4. Unir con lo existente =====================
    final_df = pd.concat([existing_df, new_df], ignore_index=True) if not existing_df.empty else new_df

    # ===================== 5. Guardar en S3 como Parquet (Snappy) =====================
    table = pa.Table.from_pandas(final_df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

    print(f"dim_date actualizado con {len(final_df)} registros en formato Snappy Parquet.")
