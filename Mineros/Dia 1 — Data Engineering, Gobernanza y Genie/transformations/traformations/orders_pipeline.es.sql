-------------------------------------------------------
-- PIPELINE DE PEDIDOS (ORDERS)
-- Lakeflow Spark Declarative Pipelines
-------------------------------------------------------
-- Este archivo es parte de un proyecto de pipeline multi‑archivo.
-- El editor del pipeline descubre automáticamente y combina
-- todos los archivos SQL de tu pipeline.
--
-- MODELO DE NOMBRES:
--   Catálogo por defecto : academia   (compartido)
--   Esquema por defecto  : <tu_apellido>   (configúralo en el pipeline)
--   La capa medallion se distingue por el SUFIJO de la tabla:
--     *_bronze, *_silver, *_gold
-- Por eso las tablas se nombran SIN esquema (heredan el esquema por defecto).
-------------------------------------------------------

-------------------------------------------------------
-- CAPA BRONZE: Ingesta de datos JSON raw
-------------------------------------------------------
-- Ingiere incrementalmente archivos JSON desde el almacenamiento
-- usando Auto Loader para procesamiento eficiente
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE orders_bronze -- hereda catálogo y esquema por defecto del pipeline
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
  format => 'json'
);

-------------------------------------------------------
-- CAPA SILVER: Limpieza y transformación
-------------------------------------------------------
-- Parsear timestamp y seleccionar columnas relevantes
-- Crea una tabla en streaming limpia y validada
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE orders_silver
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
FROM STREAM orders_bronze;

-------------------------------------------------------
-- CAPA GOLD: Agregación de negocio
-------------------------------------------------------
-- Crear una vista materializada con resumen diario de pedidos
-- Las vistas materializadas optimizan automáticamente el refresco
-------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW order_summary_gold
  COMMENT "Conteos diarios de pedidos agregados desde la capa silver"
  TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  date(order_timestamp) AS order_date,
  count(*) AS total_daily_orders,
  count(DISTINCT customer_id) AS unique_customers
FROM orders_silver
GROUP BY date(order_timestamp);

-------------------------------------------------------
-- NOTAS:
-- 1. Este archivo funciona solo o como parte de un pipeline mayor
-- 2. Las tablas de este archivo pueden ser referenciadas por otros
-- 3. Sustitución de variables: ${source} se reemplaza en tiempo de ejecución
-- 4. Las tablas en streaming usan checkpoints para procesamiento incremental
-- 5. Las vistas materializadas manejan eficientemente refrescos completos
-- 6. Todas las tablas viven en un solo esquema (academia.<usuario>);
--    la capa se identifica por el sufijo _bronze/_silver/_gold
-------------------------------------------------------
