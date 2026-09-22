-- ==========================================================================
-- GOLD: Resumen diario de transacciones por canal (CRC)
-- Vista materializada con agregaciones por fecha y canal de transacción.
-- Fuente: ${catalogo}.${user_suffix}_silver.transacciones | Destino: ${catalogo}.${user_suffix}_gold.resumen_por_canal
-- ==========================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalogo}.${user_suffix}_gold.resumen_por_canal
(
  CONSTRAINT fecha_no_nula EXPECT (fecha_transaccion IS NOT NULL),
  CONSTRAINT canal_no_nulo EXPECT (canal IS NOT NULL),
  CONSTRAINT monto_total_positivo EXPECT (monto_total_crc > 0)
)
COMMENT 'Resumen diario de transacciones por canal en colones costarricenses (CRC).'
CLUSTER BY (fecha_transaccion, canal)
AS
SELECT
  fecha_transaccion,
  canal,
  COUNT(*) AS total_transacciones,
  SUM(monto) AS monto_total_crc
FROM ${catalogo}.${user_suffix}_silver.transacciones
WHERE moneda = 'CRC'
GROUP BY ALL;
