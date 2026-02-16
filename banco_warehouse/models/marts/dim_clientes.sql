{{
  config(
    materialized = 'table',
    tags = ['gold', 'dimension']
  )
}}

SELECT
    cliente_id,
    nombre_completo,
    rut_hash,   -- aca solo llega el hash, nunca el rut real
    email_hash,
    fecha_registro
FROM {{ ref('stg_clientes') }}