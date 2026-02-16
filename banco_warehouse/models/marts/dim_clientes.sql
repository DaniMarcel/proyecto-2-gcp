{{
  config(
    materialized = 'table',
    tags = ['gold', 'dimension']
  )
}}

SELECT
    cliente_id,
    nombre_completo,
    rut_hash,   -- El RUT real nunca llega aquí
    email_hash,
    fecha_registro
FROM {{ ref('stg_clientes') }}