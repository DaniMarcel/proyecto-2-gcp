from google.cloud import storage
import os
import glob

# CONFIGURACIÓN
# PONEMOS NUESTRA ID
PROJECT_ID = "dataengineerp"
# LE PONEMOS EL NOMBRE DE NUESTRO BUCKET
BUCKET_NAME = "banco-datalake-daniel-2026"
SOURCE_FOLDER = "../data_lake"

def upload_files():
    # 1. Autenticación automática usando 'credenciales.json' (si está en el entorno)
    client = storage.Client(project=PROJECT_ID)
    
    # 2. Crear o conectar con el bucket
    bucket = client.bucket(BUCKET_NAME)
    if not bucket.exists():
        print(f"🔨 Creando bucket {BUCKET_NAME} en US...")
        bucket.create(location="US")
    else:
        print(f"✅ Bucket {BUCKET_NAME} detectado.")

    # 3. Buscar CSVs
    files = glob.glob(f"{SOURCE_FOLDER}/*.csv")
    if not files:
        print("⚠️ ¡Alerta! No hay archivos CSV en la carpeta 'data_lake'.")
        return

    # 4. Subir (Upload)
    for file_path in files:
        filename = os.path.basename(file_path)
        blob = bucket.blob(filename) # El nombre que tendrá en la nube
        
        print(f"🚀 Subiendo {filename}...")
        blob.upload_from_filename(file_path)
        print(f"   ✨ {filename} cargado ok.")

if __name__ == "__main__":
    # Truco para no tener que configurar variables de entorno en Windows manualmente
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credenciales.json"
    upload_files()