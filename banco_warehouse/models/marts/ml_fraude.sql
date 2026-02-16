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
    -- Features (Datos para aprender)
    monto,
    metodo_pago,
    EXTRACT(HOUR FROM fecha_transaccion) AS hora_del_dia, 
    
    -- Label (La respuesta correcta)
    es_fraude

FROM {{ ref('fact_transacciones') }}
WHERE es_fraude IS NOT NULL