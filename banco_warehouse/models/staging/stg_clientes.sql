WITH source AS (
    SELECT * FROM {{ source('banco_origen', 'raw_clientes') }}
),

renamed AS (
    SELECT
        -- id del cliente
        cliente_id,
        
        -- pasamos nombres a mayuscula para estandarizar
        UPPER(nombre) AS nombre_completo,
        
        -- hasheamos el rut para no guardar el dato real
        TO_HEX(SHA256(rut)) AS rut_hash,
        
        -- lo mismo con el email, asi podemos cruzar sin exponer info
        TO_HEX(SHA256(email)) AS email_hash,
        
        -- convertimos la fecha de string a date
        -- usamos SAFE por si viene algun dato malo que no rompa todo
        SAFE.PARSE_DATE('%Y-%m-%d', fecha_registro) AS fecha_registro

    FROM source
)

SELECT * FROM renamed