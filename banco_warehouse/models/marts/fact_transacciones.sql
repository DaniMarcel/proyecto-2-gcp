{{
  config(
    materialized = 'table',
    partition_by = {
      "field": "fecha_transaccion",
      "data_type": "timestamp",
      "granularity": "day"
    },
    tags = ['gold', 'fact']
  )
}}

SELECT
    t.transaccion_id,
    t.cliente_id,
    t.comercio_id,
    t.fecha_transaccion,
    t.monto,
    t.metodo_pago,
    t.es_fraude,
    
    -- 🧠 Lógica de Negocio Agregada: Categorización de Riesgo
    CASE 
        WHEN t.monto > 1000000 THEN 'CRÍTICO'
        WHEN t.monto > 500000 THEN 'ALTO'
        ELSE 'NORMAL'
    END AS nivel_riesgo_monto

FROM {{ ref('stg_transacciones') }} t