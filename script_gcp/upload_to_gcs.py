from google.cloud import storage
import os
import glob

# mis datos de GCP
PROJECT_ID = "dataengineerp"
BUCKET_NAME = "banco-datalake-daniel-2026"
SOURCE_FOLDER = "../data_lake"

def upload_files():
    # nos conectamos al proyecto
    client = storage.Client(project=PROJECT_ID)
    
    # vemos si el bucket ya existe, si no lo creamos
    bucket = client.bucket(BUCKET_NAME)
    if not bucket.exists():
        print(f"Creando bucket {BUCKET_NAME} en US...")
        bucket.create(location="US")
    else:
        print(f"Bucket {BUCKET_NAME} detectado.")

    # buscamos los csv que generamos antes
    files = glob.glob(f"{SOURCE_FOLDER}/*.csv")
    if not files:
        print("No hay archivos CSV en la carpeta 'data_lake'.")
        return

    # subimos cada archivo
    for file_path in files:
        filename = os.path.basename(file_path)
        blob = bucket.blob(filename) # nombre que va a tener en la nube
        
        print(f"Subiendo {filename}...")
        blob.upload_from_filename(file_path)
        print(f"   {filename} cargado ok.")

if __name__ == "__main__":
    # seteamos las credenciales aca directo para no tener que hacerlo en el sistema
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credenciales.json"
    upload_files()