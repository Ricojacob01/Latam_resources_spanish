-- ==========================================================================
-- GOLD: Agregaciones de negocio para analítica bancaria BNCR
-- ==========================================================================

-- Resumen diario por sucursal
CREATE OR REFRESH MATERIALIZED VIEW resumen_diario_sucursal (
  fecha_transaccion DATE COMMENT "Fecha de las transacciones.",
  sucursal_id STRING COMMENT "Código de sucursal.",
  nombre_sucursal STRING COMMENT "Nombre de la sucursal.",
  provincia STRING COMMENT "Provincia.",
  total_transacciones BIGINT COMMENT "Número total de transacciones.",
  monto_total DECIMAL(19,2) COMMENT "Monto total transaccionado (CRC).",
  monto_promedio DECIMAL(19,2) COMMENT "Monto promedio por transacción.",
  clientes_unicos BIGINT COMMENT "Clientes únicos que transaccionaron."
)
COMMENT "Métricas diarias de transacciones por sucursal BNCR."
AS SELECT
  t.fecha_transaccion,
  t.sucursal_id,
  s.nombre AS nombre_sucursal,
  s.provincia,
  COUNT(*) AS total_transacciones,
  SUM(t.monto) AS monto_total,
  AVG(t.monto) AS monto_promedio,
  COUNT(DISTINCT t.cliente_id) AS clientes_unicos
FROM transacciones t
LEFT JOIN sucursales s ON t.sucursal_id = s.sucursal_id
GROUP BY ALL;

-- Resumen por producto y canal
CREATE OR REFRESH MATERIALIZED VIEW resumen_producto_canal (
  fecha_transaccion DATE COMMENT "Fecha.",
  producto STRING COMMENT "Producto bancario.",
  canal STRING COMMENT "Canal de transacción.",
  total_transacciones BIGINT COMMENT "Total de transacciones.",
  monto_total DECIMAL(19,2) COMMENT "Monto total (CRC).",
  monto_maximo DECIMAL(19,2) COMMENT "Transacción de mayor monto."
)
COMMENT "Métricas por producto bancario y canal de atención."
AS SELECT
  fecha_transaccion,
  producto,
  canal,
  COUNT(*) AS total_transacciones,
  SUM(monto) AS monto_total,
  MAX(monto) AS monto_maximo
FROM transacciones
GROUP BY ALL;

-- Métricas de clientes por segmento
CREATE OR REFRESH MATERIALIZED VIEW metricas_clientes (
  segmento STRING COMMENT "Segmento del cliente.",
  total_clientes BIGINT COMMENT "Total de clientes activos.",
  saldo_promedio DECIMAL(19,2) COMMENT "Saldo promedio de cuentas.",
  transacciones_promedio BIGINT COMMENT "Transacciones promedio por cliente."
)
COMMENT "Métricas agregadas por segmento de cliente."
AS SELECT
  c.segmento,
  COUNT(DISTINCT c.customer_id) AS total_clientes,
  AVG(cta.saldo) AS saldo_promedio,
  COUNT(DISTINCT t.transaccion_id) / COUNT(DISTINCT c.customer_id) AS transacciones_promedio
FROM clientes c
LEFT JOIN cuentas cta ON c.customer_id = cta.cliente_id
LEFT JOIN transacciones t ON c.customer_id = t.cliente_id
GROUP BY c.segmento;
