WITH source AS (
    SELECT * FROM {{ source('banco_origen', 'raw_transacciones') }}
),

renamed AS (
    SELECT
        transaccion_id,
        cliente_id,   -- Foreign Key
        comercio_id,  -- Foreign Key
        
        -- Asegurar que el monto sea número entero
        SAFE_CAST(monto AS INT64) AS monto,
        
        -- 🕒 TRUCO PRO: Convertir String a Timestamp
        -- BigQuery es inteligente, SAFE_CAST suele funcionar con formato ISO.
        -- Si falla, devuelve NULL en lugar de romper el pipeline.
        SAFE_CAST(fecha AS TIMESTAMP) AS fecha_transaccion,
        
        metodo_pago,
        
        -- El flag de fraude lo dejamos como entero (1 o 0) para usarlo en Machine Learning después
        SAFE_CAST(es_fraude AS INT64) AS es_fraude

    FROM source
)

SELECT * FROM renamed