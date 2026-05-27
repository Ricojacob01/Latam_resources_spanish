-------------------------------------------------------
-- PIPELINE DE PEDIDOS (ORDERS)
-- Lakeflow Spark Declarative Pipelines
-------------------------------------------------------
-- Este archivo es parte de un proyecto de pipeline multi‑archivo.
-- El editor del pipeline descubre automáticamente y combina
-- todos los archivos SQL de tu pipeline.
-------------------------------------------------------

-------------------------------------------------------
-- CAPA BRONZE: Ingesta de datos JSON raw
-------------------------------------------------------
-- Ingiere incrementalmente archivos JSON desde el almacenamiento
-- usando Auto Loader para procesamiento eficiente
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze.orders -- hereda catálogo por defecto pero especifica esquema bronze
  COMMENT "Datos de pedidos raw ingeridos desde archivos JSON"
  TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.reset.allowed" = false  -- Evitar refrescos completos accidentales
  )
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files( -- Procesa incrementalmente archivos nuevos con Auto Loader
  "${source}/orders",  -- Usa la variable de configuración 'source' del pipeline
  format => 'json"
);

-------------------------------------------------------
-- CAPA SILVER: Limpieza y transformación
-------------------------------------------------------
-- Parsear timestamp y seleccionar columnas relevantes
-- Crea una tabla en streaming limpia y validada
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver.orders_clean -- Publica a múltiples catálogos y esquemas
  (
    -- Expectativas de calidad de datos
    CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL),
    CONSTRAINT valid_timestamp EXPECT (order_timestamp > "2020-01-01")
  )
  COMMENT "Datos de pedidos limpios con campos validados"
  TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
  order_id,
  timestamp(order_timestamp) AS order_timestamp,
  customer_id,
  notifications
FROM STREAM bronze.orders;

-------------------------------------------------------
-- CAPA GOLD: Agregación de negocio
-------------------------------------------------------
-- Crear una vista materializada con resumen diario de pedidos
-- Las vistas materializadas optimizan automáticamente el refresco
-------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold.order_summary
  COMMENT "Conteos diarios de pedidos agregados desde la capa silver"
  TBLPROPERTIES ("quality" = "gold")
AS 
SELECT 
  date(order_timestamp) AS order_date,
  count(*) AS total_daily_orders,
  count(DISTINCT customer_id) AS unique_customers
FROM silver.orders_clean
GROUP BY date(order_timestamp);

-------------------------------------------------------
-- NOTAS:
-- 1. Este archivo funciona solo o como parte de un pipeline mayor
-- 2. Las tablas de este archivo pueden ser referenciadas por otros
-- 3. Sustitución de variables: ${source} se reemplaza en tiempo de ejecución
-- 4. Las tablas en streaming usan checkpoints para procesamiento incremental
-- 5. Las vistas materializadas manejan eficientemente refrescos completos
-------------------------------------------------------

