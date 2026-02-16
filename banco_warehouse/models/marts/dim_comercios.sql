{{
  config(
    materialized = 'table',
    tags = ['gold', 'dimension']
  )
}}

SELECT
    comercio_id,
    nombre_comercio,
    rubro,
    ciudad
FROM {{ ref('stg_comercios') }}