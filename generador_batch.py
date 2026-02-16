import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# parametros generales del script
NUM_CLIENTES = 100
NUM_COMERCIOS = 20
NUM_TRANSACCIONES = 5000  # cantidad de compras que vamos a generar
fake = Faker('es_CL')     # usamos locale chileno para los datos

print("Iniciando generacion del Data Lake Batch...")

# --- 1. Generamos la tabla de clientes ---
print("Generando Clientes...")
clientes = []
for _ in range(NUM_CLIENTES):
    cliente = {
        "cliente_id": fake.uuid4(),
        "nombre": fake.name(),
        "rut": fake.ssn(),          # esto despues hay que hashearlo porque es dato sensible
        "email": fake.email(),      # idem, tambien es PII
        "fecha_registro": fake.date_between(start_date='-5y', end_date='today')
    }
    clientes.append(cliente)

df_clientes = pd.DataFrame(clientes)

# --- 2. Generamos la tabla de comercios ---
print("Generando Comercios...")
rubros = ["Supermercado", "Combustible", "Retail", "Restaurante", "Tecnología", "Viajes"]
comercios = []
for _ in range(NUM_COMERCIOS):
    comercio = {
        "comercio_id": fake.uuid4(),
        "nombre_comercio": fake.company(),
        "rubro": random.choice(rubros),
        "ciudad": fake.city()
    }
    comercios.append(comercio)

df_comercios = pd.DataFrame(comercios)

# --- 3. Transacciones (tabla de hechos) ---
print("Generando Transacciones con patrones de Fraude y Errores...")
transacciones = []

for _ in range(NUM_TRANSACCIONES):
    # tomamos un cliente y comercio que ya existan para mantener la relacion
    cliente = random.choice(clientes)
    comercio = random.choice(comercios)
    
    # aca simulamos fraude
    es_fraude = False
    monto = random.randint(1000, 150000)
    
    # fraude tipo 1: montos gigantes en retail o viajes
    if comercio['rubro'] in ['Viajes', 'Tecnología'] and random.random() < 0.1:
        monto = random.randint(500000, 3000000)
        es_fraude = True
    
    # fraude tipo 2: un porcentaje aleatorio chico
    if random.random() < 0.02: # 2% fraude random
        es_fraude = True

    # --- metemos datos sucios a proposito para limpiarlos con dbt despues ---
    fecha_tx = fake.date_time_between(start_date='-1y', end_date='now')
    metodo_pago = "Tarjeta de Crédito"
    
    # dato sucio 1: montos negativos (1%)
    if random.random() < 0.01:
        monto = monto * -1 
    
    # dato sucio 2: fechas futuras (0.5%)
    if random.random() < 0.005:
        fecha_tx = datetime.now() + timedelta(days=365)

    # dato sucio 3: metodo de pago null
    if random.random() < 0.02:
        metodo_pago = None

    tx = {
        "transaccion_id": fake.uuid4(),
        "cliente_id": cliente['cliente_id'],   # fk a clientes
        "comercio_id": comercio['comercio_id'], # fk a comercios
        "fecha": fecha_tx,
        "monto": monto,
        "metodo_pago": metodo_pago,
        "es_fraude": 1 if es_fraude else 0 # para usar como label en ML
    }
    transacciones.append(tx)

df_transacciones = pd.DataFrame(transacciones)

# --- 4. Guardamos todo en CSV (esto simula el data lake) ---
os.makedirs("data_lake", exist_ok=True)

df_clientes.to_csv("data_lake/clientes_master.csv", index=False)
df_comercios.to_csv("data_lake/comercios_master.csv", index=False)
df_transacciones.to_csv("data_lake/transacciones_diarias.csv", index=False)

print(f"Listo! Archivos generados en la carpeta 'data_lake':")
print(f"   - Clientes: {len(df_clientes)} registros")
print(f"   - Comercios: {len(df_comercios)} registros")
print(f"   - Transacciones: {len(df_transacciones)} registros")