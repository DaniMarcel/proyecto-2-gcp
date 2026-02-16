{{
  config(
    materialized = 'table',
    tags = ['ml', 'prediccion']
  )
}}

SELECT
    *
FROM
  ML.PREDICT(MODEL {{ target.schema }}.modelo_detector_fraude,
    (
    SELECT 
        transaccion_id,
        monto,
        metodo_pago,
        EXTRACT(HOUR FROM fecha_transaccion) AS hora_del_dia,
        es_fraude AS etiqueta_real -- lo dejamos para comparar con la prediccion
    FROM {{ ref('fact_transacciones') }}
    )
  )