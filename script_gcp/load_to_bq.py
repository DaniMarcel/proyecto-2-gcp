from google.cloud import bigquery
import os

# --- CONFIGURACIÓN ---
PROJECT_ID = "dataengineerp"
DATASET_ID = "banco_raw"
# ¡Asegúrate de que este sea tu bucket correcto!
BUCKET_NAME = "banco-datalake-daniel-2026" 

def load_table(table_name, file_name, schema):
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table(table_name)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1, # Saltamos la cabecera del CSV porque ya damos el esquema nosotros
        schema=schema,       # <--- AQUÍ ESTÁ LA CORRECCIÓN: Esquema explícito
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    uri = f"gs://{BUCKET_NAME}/{file_name}"
    print(f"⏳ Cargando {file_name} en {table_name}...")
    
    try:
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()  # Esperamos
        
        table = client.get_table(table_ref)
        print(f"✅ {table_name}: {table.num_rows} filas cargadas correctamente.")
    except Exception as e:
        print(f"❌ Error cargando {table_name}: {e}")

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credenciales.json"
    
    # 1. Esquema para CLIENTES
    schema_clientes = [
        bigquery.SchemaField("cliente_id", "STRING"),
        bigquery.SchemaField("nombre", "STRING"),
        bigquery.SchemaField("rut", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("fecha_registro", "STRING"), # Lo cargamos como String y dbt lo arregla a Date
    ]
    load_table("raw_clientes", "clientes_master.csv", schema_clientes)
    
    # 2. Esquema para COMERCIOS (El que falló)
    schema_comercios = [
        bigquery.SchemaField("comercio_id", "STRING"),
        bigquery.SchemaField("nombre_comercio", "STRING"),
        bigquery.SchemaField("rubro", "STRING"),
        bigquery.SchemaField("ciudad", "STRING"),
    ]
    load_table("raw_comercios", "comercios_master.csv", schema_comercios)
    
    # 3. Esquema para TRANSACCIONES
    schema_transacciones = [
        bigquery.SchemaField("transaccion_id", "STRING"),
        bigquery.SchemaField("cliente_id", "STRING"),
        bigquery.SchemaField("comercio_id", "STRING"),
        bigquery.SchemaField("fecha", "STRING"), # Timestamp como String primero
        bigquery.SchemaField("monto", "INTEGER"),
        bigquery.SchemaField("metodo_pago", "STRING"),
        bigquery.SchemaField("es_fraude", "INTEGER"),
    ]
    load_table("raw_transacciones", "transacciones_diarias.csv", schema_transacciones)