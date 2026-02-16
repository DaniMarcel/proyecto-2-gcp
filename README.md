# Modern Data Warehouse & Fraud Detection (Batch + ML)

**Arquitectura Medallion con dbt & BigQuery ML**

Este proyecto implementa un Data Warehouse completo para una Fintech, procesando cargas batch diarias, aplicando gobernanza de datos (hashing de PII), modelado dimensional (Star Schema) y detección de fraude predictiva utilizando Machine Learning nativo en SQL.

## Arquitectura Técnica

- **Ingesta:** Scripts Python para simular data transaccional compleja y carga a Google Cloud Storage (Data Lake).
- **Data Warehouse:** Google BigQuery.
- **Transformación (ELT):** **dbt (data build tool)** implementando arquitectura de capas (Bronze/Silver/Gold).
- **Machine Learning:** **BigQuery ML** (Regresión Logística) entrenado y ejecutado directamente mediante SQL y dbt hooks.
- **Seguridad:** Enmascaramiento de PII (RUT, Email) mediante hashing SHA-256 en la capa de Staging.

## Flujo de Datos (Pipeline)

1. **Raw Layer (Bronze):** Ingesta de archivos CSV (Clientes, Comercios, Transacciones) sin procesar.
2. **Staging Layer (Silver):**
   - Limpieza de tipos de datos.
   - Estandarización de textos.
   - **Hashing criptográfico** de datos sensibles.
3. **Marts Layer (Gold):**
   - **Star Schema:** Tablas de Dimensiones (`dim_clientes`, `dim_comercios`) y Hechos (`fact_transacciones`).
   - **Particionamiento:** Optimización de costos particionando por fecha.
4. **ML Layer (Intelligence):**
   - Entrenamiento de modelo `LOGISTIC_REG` para detectar patrones de fraude.
   - Generación de tabla de predicciones (`predicciones_flat`) con probabilidades por transacción.

## Tecnologías

- Python 3.11 (Pandas, Faker, Google-Cloud-Storage).
- SQL (Standard SQL BigQuery).
- dbt Core 1.8.
- Google BigQuery ML.

---

_Proyecto desarrollado como parte de mi aprendizaje practico en Ingeniería de Datos._
