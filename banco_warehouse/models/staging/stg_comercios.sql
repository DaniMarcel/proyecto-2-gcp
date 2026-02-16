WITH source AS (
    SELECT * FROM {{ source('banco_origen', 'raw_comercios') }}
),

renamed AS (
    SELECT
        comercio_id,
        
        -- Estandarización: Todo a mayúsculas para evitar duplicados por "Lider" vs "LIDER"
        UPPER(nombre_comercio) AS nombre_comercio,
        UPPER(rubro) AS rubro,
        UPPER(ciudad) AS ciudad

    FROM source
)

SELECT * FROM renamed