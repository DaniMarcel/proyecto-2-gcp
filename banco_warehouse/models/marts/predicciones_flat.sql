{{
  config(
    materialized = 'table'
  )
}}

WITH predicciones AS (
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
            es_fraude AS etiqueta_real
        FROM {{ ref('fact_transacciones') }}
        )
      )
)

SELECT
    transaccion_id,
    monto,
    hora_del_dia,
    etiqueta_real,
    predicted_es_fraude,
    -- Aquí está el truco: Extraemos solo la probabilidad de que SEA fraude (label 1)
    p.prob as probabilidad_fraude
FROM predicciones,
UNNEST(predicted_es_fraude_probs) AS p
WHERE p.label = 1