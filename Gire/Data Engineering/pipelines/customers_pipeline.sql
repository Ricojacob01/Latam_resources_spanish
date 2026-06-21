-- =============================================================================
-- customers_pipeline.sql — Spark Declarative Pipeline · CDC de CLIENTES (SCD Tipo 1)
-- =============================================================================
-- Se agrega al MISMO pipeline que orders_pipeline.sql (el pipeline auto-descubre
-- los archivos de la carpeta de transformaciones). Demuestra AUTO CDC INTO:
-- aplica INSERT/UPDATE/DELETE a la tabla destino sin escribir MERGE a mano.
--
-- Eventos fuente en ${source}/customers/00.json: 20 INSERT, 5 UPDATE, 2 DELETE
--   => silver_customers debe quedar con 18 clientes activos (20 - 2 borrados).
-- =============================================================================

-- 🥉 BRONZE — eventos CDC crudos
CREATE OR REFRESH STREAMING TABLE bronze_customers_raw
  COMMENT "Eventos CDC crudos de clientes"
  TBLPROPERTIES ("quality" = "bronze", "pipelines.reset.allowed" = false)
AS SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name  AS source_file
FROM STREAM read_files("${source}/customers", format => 'json');

-- 🥈 BRONZE-CLEAN — eventos CDC validados (nota: el email puede ser NULL si es DELETE)
CREATE OR REFRESH STREAMING TABLE bronze_customers_clean
  (
    CONSTRAINT valid_id        EXPECT (customer_id IS NOT NULL)  ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_operation EXPECT (operation IS NOT NULL)    ON VIOLATION DROP ROW,
    CONSTRAINT valid_email     EXPECT (
        email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
        OR operation = 'DELETE'
    ) ON VIOLATION DROP ROW
  )
  COMMENT "Eventos CDC validados, listos para aplicar"
AS SELECT
  *,
  CAST(from_unixtime(timestamp) AS timestamp) AS timestamp_datetime
FROM STREAM bronze_customers_raw;

-- 🥈 SILVER — tabla destino (la gestiona el FLOW de abajo)
CREATE OR REFRESH STREAMING TABLE silver_customers
  COMMENT "Estado actual de clientes (SCD Tipo 1)";

-- 🔄 FLOW — aplica el CDC declarativamente
CREATE FLOW customers_cdc_flow AS
AUTO CDC INTO silver_customers
FROM STREAM bronze_customers_clean
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = 'DELETE'
  SEQUENCE BY timestamp_datetime
  COLUMNS * EXCEPT (operation, timestamp, processing_time, source_file, _rescued_data)
  STORED AS SCD TYPE 1;

-- 🥇 GOLD — resumen de clientes activos
CREATE OR REFRESH MATERIALIZED VIEW gold_customer_summary
  COMMENT "Clientes activos por ciudad"
AS SELECT city, count(*) AS clientes_activos
FROM silver_customers
GROUP BY city;
