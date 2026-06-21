-- =============================================================================
-- orders_pipeline.sql — Spark Declarative Pipeline (Lakeflow) · medallion de PEDIDOS
-- =============================================================================
-- Este archivo es la DEFINICIÓN (código) del pipeline. Se adjunta a un pipeline
-- de Lakeflow desde la UI (módulo 03). Requiere una variable de configuración:
--
--   Clave:  source
--   Valor:  /Volumes/ardemo_classic_dnubtw_catalog/ws_<tu_usuario>/raw_de
--
-- El pipeline infiere el grafo bronze -> silver -> gold por las referencias entre
-- tablas y gestiona orden, checkpoints y procesamiento incremental por ti.
-- =============================================================================

-- 🥉 BRONZE — ingesta incremental de archivos JSON (Auto Loader vía read_files)
CREATE OR REFRESH STREAMING TABLE bronze_orders
  COMMENT "Pedidos crudos ingeridos incrementalmente desde JSON"
  TBLPROPERTIES ("quality" = "bronze", "pipelines.reset.allowed" = false)
AS SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name  AS source_file
FROM STREAM read_files("${source}/orders", format => 'json');

-- 🥈 SILVER — limpieza + EXPECTATIONS (calidad de datos declarativa)
CREATE OR REFRESH STREAMING TABLE silver_orders_clean
  (
    CONSTRAINT valid_order_id  EXPECT (order_id IS NOT NULL)         ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_customer  EXPECT (customer_id IS NOT NULL)      ON VIOLATION DROP ROW,
    CONSTRAINT valid_timestamp EXPECT (order_timestamp > '2020-01-01'),
    CONSTRAINT valid_amount    EXPECT (amount > 0)                   ON VIOLATION DROP ROW
  )
  COMMENT "Pedidos limpios y validados (capa silver)"
  TBLPROPERTIES ("quality" = "silver")
AS SELECT
  order_id,
  timestamp(order_timestamp) AS order_timestamp,
  customer_id,
  amount
FROM STREAM bronze_orders;

-- 🥇 GOLD — agregación incremental de negocio (vista materializada)
CREATE OR REFRESH MATERIALIZED VIEW gold_order_summary
  COMMENT "Conteos e ingresos diarios agregados desde silver"
  TBLPROPERTIES ("quality" = "gold")
AS SELECT
  date(order_timestamp)        AS order_date,
  count(*)                     AS total_daily_orders,
  count(DISTINCT customer_id)  AS unique_customers,
  round(sum(amount), 2)        AS daily_revenue
FROM silver_orders_clean
GROUP BY date(order_timestamp);
