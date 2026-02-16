WITH source AS (
    SELECT * FROM {{ source('banco_origen', 'raw_clientes') }}
),

renamed AS (
    SELECT
        -- Generamos un ID interno seguro
        cliente_id,
        
        -- Limpieza básica: Nombres siempre en mayúsculas
        UPPER(nombre) AS nombre_completo,
        
        -- 🛡️ SEGURIDAD (Hashing SHA-256)
        -- Convertimos el RUT real en un código alfanumérico ilegible
        TO_HEX(SHA256(rut)) AS rut_hash,
        
        -- Convertimos el Email real en un hash para poder cruzar datos sin exponer al usuario
        TO_HEX(SHA256(email)) AS email_hash,
        
        -- Casteo de tipo de dato (String -> Date)
        -- Usamos SAFE.CAST por si viene alguna fecha basura, que no rompa el pipeline (devuelve null)
        SAFE.PARSE_DATE('%Y-%m-%d', fecha_registro) AS fecha_registro

    FROM source
)

SELECT * FROM renamed