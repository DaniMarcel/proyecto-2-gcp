WITH source AS (
    SELECT * FROM {{ source('banco_origen', 'raw_comercios') }}
),

renamed AS (
    SELECT
        comercio_id,
        
        -- todo a mayusculas para que no haya duplicados tipo "Lider" y "LIDER"
        UPPER(nombre_comercio) AS nombre_comercio,
        UPPER(rubro) AS rubro,
        UPPER(ciudad) AS ciudad

    FROM source
)

SELECT * FROM renamed