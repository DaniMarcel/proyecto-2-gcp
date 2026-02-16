WITH source AS (
    SELECT * FROM {{ source('banco_origen', 'raw_transacciones') }}
),

renamed AS (
    SELECT
        transaccion_id,
        cliente_id,   -- fk a clientes
        comercio_id,  -- fk a comercios
        
        -- nos aseguramos que el monto sea entero
        SAFE_CAST(monto AS INT64) AS monto,
        
        -- convertimos el string a timestamp
        -- SAFE_CAST devuelve null si el dato viene malo en vez de tirar error
        SAFE_CAST(fecha AS TIMESTAMP) AS fecha_transaccion,
        
        metodo_pago,
        
        -- dejamos el flag de fraude como entero (1/0) para usarlo en el modelo de ML
        SAFE_CAST(es_fraude AS INT64) AS es_fraude

    FROM source
)

SELECT * FROM renamed