{{
  config(
    materialized = 'view',
    tags = ['ml', 'fraude'],
    post_hook = "
      CREATE OR REPLACE MODEL `{{ target.project }}.{{ target.schema }}.modelo_detector_fraude`
      OPTIONS(
        model_type='LOGISTIC_REG',
        input_label_cols=['es_fraude'],
        max_iterations=10
      ) AS
      SELECT * FROM {{ this }}
    "
  )
}}

SELECT
    -- estas son las features que le damos al modelo
    monto,
    metodo_pago,
    EXTRACT(HOUR FROM fecha_transaccion) AS hora_del_dia, 
    
    -- este es el label, o sea la respuesta que queremos predecir
    es_fraude

FROM {{ ref('fact_transacciones') }}
WHERE es_fraude IS NOT NULL