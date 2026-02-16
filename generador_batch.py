import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# Configuración
NUM_CLIENTES = 100
NUM_COMERCIOS = 20
NUM_TRANSACCIONES = 5000  # Simularemos 5000 compras
fake = Faker('es_CL')     # Datos chilenos reales

print("🚀 Iniciando generación del Data Lake Batch...")

# --- 1. Generar Clientes (DIM_CLIENTE) ---
print("👤 Generando Clientes...")
clientes = []
for _ in range(NUM_CLIENTES):
    cliente = {
        "cliente_id": fake.uuid4(),
        "nombre": fake.name(),
        "rut": fake.ssn(),          # PII (Dato Sensible)
        "email": fake.email(),      # PII (Dato Sensible)
        "fecha_registro": fake.date_between(start_date='-5y', end_date='today')
    }
    clientes.append(cliente)

df_clientes = pd.DataFrame(clientes)

# --- 2. Generar Comercios (DIM_COMERCIO) ---
print("buildings Generando Comercios...")
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

# --- 3. Generar Transacciones (FACT_TRANSACCIONES) ---
print("💳 Generando Transacciones con patrones de Fraude y Errores...")
transacciones = []

for _ in range(NUM_TRANSACCIONES):
    # Seleccionamos un cliente y comercio EXISTENTE (Integridad Referencial)
    cliente = random.choice(clientes)
    comercio = random.choice(comercios)
    
    # Lógica de Fraude Simulado
    es_fraude = False
    monto = random.randint(1000, 150000)
    
    # Patrón de Fraude 1: Montos muy altos en Retail o Viajes
    if comercio['rubro'] in ['Viajes', 'Tecnología'] and random.random() < 0.1:
        monto = random.randint(500000, 3000000)
        es_fraude = True
    
    # Patrón de Fraude 2: Transacciones rápidas (lo simularemos con flags por ahora)
    if random.random() < 0.02: # 2% de fraude aleatorio
        es_fraude = True

    # --- INYECCIÓN DE DATOS SUCIOS (Para limpiar con dbt) ---
    fecha_tx = fake.date_time_between(start_date='-1y', end_date='now')
    metodo_pago = "Tarjeta de Crédito"
    
    # Error 1: Montos Negativos (1% de probabilidad)
    if random.random() < 0.01:
        monto = monto * -1 
    
    # Error 2: Fechas Futuras (0.5% de probabilidad)
    if random.random() < 0.005:
        fecha_tx = datetime.now() + timedelta(days=365)

    # Error 3: Nulos en método de pago
    if random.random() < 0.02:
        metodo_pago = None

    tx = {
        "transaccion_id": fake.uuid4(),
        "cliente_id": cliente['cliente_id'],   # Foreign Key
        "comercio_id": comercio['comercio_id'], # Foreign Key
        "fecha": fecha_tx,
        "monto": monto,
        "metodo_pago": metodo_pago,
        "es_fraude": 1 if es_fraude else 0 # Label para ML
    }
    transacciones.append(tx)

df_transacciones = pd.DataFrame(transacciones)

# --- 4. Guardar en CSV (Simulando nuestro Data Lake) ---
os.makedirs("data_lake", exist_ok=True) # Carpeta contenedora

df_clientes.to_csv("data_lake/clientes_master.csv", index=False)
df_comercios.to_csv("data_lake/comercios_master.csv", index=False)
df_transacciones.to_csv("data_lake/transacciones_diarias.csv", index=False)

print(f"✅ ¡Listo! Archivos generados en la carpeta 'data_lake':")
print(f"   - Clientes: {len(df_clientes)} registros")
print(f"   - Comercios: {len(df_comercios)} registros")
print(f"   - Transacciones: {len(df_transacciones)} registros")