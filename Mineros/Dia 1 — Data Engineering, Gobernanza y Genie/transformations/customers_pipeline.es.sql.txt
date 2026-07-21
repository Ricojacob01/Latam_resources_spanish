-------------------------------------------------------
-- PIPELINE DE CLIENTES CON AUTO CDC (ESPAÑOL)
-- Lakeflow Spark Declarative Pipelines
-------------------------------------------------------
-- Este archivo demuestra Change Data Capture (CDC)
-- usando la funcionalidad AUTO CDC INTO para SCD Tipo 1
-- 
-- AUTO CDC maneja automáticamente:
-- - Operaciones INSERT (nuevos clientes)
-- - Operaciones UPDATE (cambios de clientes)
-- - Operaciones DELETE (eliminaciones de clientes)
-------------------------------------------------------

-------------------------------------------------------
-- CAPA BRONZE: Ingesta de datos CDC raw
-------------------------------------------------------
-- Ingerir eventos CDC desde el sistema fuente
-- Cada registro contiene: campos + tipo de operación
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze.customers_raw
  COMMENT "Eventos CDC raw para datos de clientes"
  TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.reset.allowed" = false
  )
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  "${source}/customers",
  format => 'json'
);

-------------------------------------------------------
-- CAPA BRONZE: Limpieza y validación de CDC
-------------------------------------------------------
-- Agregar verificaciones de calidad antes de aplicar CDC
-- Transformar timestamp para secuenciamiento correcto
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze.customers_clean
  (
    -- Validaciones críticas - fallar la actualización si se violan
    CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
    
    -- Reglas de negocio - dependen de la operación
    CONSTRAINT valid_name EXPECT (name IS NOT NULL OR operation = 'DELETE'),
    CONSTRAINT valid_address EXPECT (
      (address IS NOT NULL AND 
       city IS NOT NULL AND 
       state IS NOT NULL AND 
       zip_code IS NOT NULL) OR
      operation = 'DELETE'
    ),
    
    -- Validación de formato de email - descarta filas inválidas
    CONSTRAINT valid_email EXPECT (
      rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') OR 
      operation = 'DELETE'
    ) ON VIOLATION DROP ROW
  )
  COMMENT "Eventos CDC validados listos para procesamiento"
  TBLPROPERTIES ("quality" = "bronze")
AS 
SELECT 
  *,
  -- Convertir Unix timestamp a timestamp para secuenciar
  CAST(from_unixtime(timestamp) AS timestamp) AS timestamp_datetime
FROM STREAM bronze.customers_raw;

-------------------------------------------------------
-- CAPA SILVER: Aplicar CDC con AUTO CDC INTO
-------------------------------------------------------
-- Crear tabla destino para SCD Tipo 1
-- Será actualizada automáticamente por el flujo CDC
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver.customers
  COMMENT "Estado actual de clientes (SCD Tipo 1)";

-------------------------------------------------------
-- CREATE FLOW: Procesamiento Auto CDC
-------------------------------------------------------
-- El flujo procesa operaciones CDC automáticamente:
-- - INSERT: Agrega nuevos clientes
-- - UPDATE: Modifica clientes existentes (sobrescribe)
-- - DELETE: Elimina clientes en la tabla destino
-------------------------------------------------------

CREATE FLOW customers_cdc_flow AS 
AUTO CDC INTO silver.customers                 -- Tabla destino a mantener
FROM STREAM bronze.customers_clean             -- Eventos CDC origen
  KEYS (customer_id)                           -- Clave primaria para hacer match
  APPLY AS DELETE WHEN operation = 'DELETE'    -- Manejar eliminaciones
  SEQUENCE BY timestamp_datetime               -- Orden de operaciones
  COLUMNS * EXCEPT (timestamp, _rescued_data, operation, processing_time, source_file)
  STORED AS SCD TYPE 1;                        -- Slowly Changing Dimension Tipo 1


-------------------------------------------------------
-- CAPA GOLD: Analítica de clientes
-------------------------------------------------------
-- Crear vistas de negocio desde el estado actual
-------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold.customer_summary
  COMMENT "Resumen de clientes con métricas derivadas"
  TBLPROPERTIES ("quality" = "gold")
AS 
SELECT 
  customer_id,
  name,
  state,
  city,
  -- Aquí pueden agregarse joins con pedidos/estatus si es necesario
  current_timestamp() AS last_refreshed
FROM silver.customers;

-------------------------------------------------------
-- CONCEPTOS CLAVE: AUTO CDC INTO
-- 
-- 1. Operaciones CDC:
--    - operation = 'INSERT': Registro nuevo
--    - operation = 'UPDATE': Registro modificado
--    - operation = 'DELETE': Registro eliminado
-- 
-- 2. Cláusula KEYS:
--    - Define la clave primaria para hacer match
--    - Identifica la fila a actualizar/eliminar
--    - Puede ser simple o compuesta
-- 
-- 3. SEQUENCE BY:
--    - Determina el orden de aplicación de cambios
--    - Crítico para eventos fuera de orden
-- 
-- 4. SCD TYPE 1 vs TYPE 2:
--    - TYPE 1: Sobrescribe valores (solo estado actual)
--    - TYPE 2: Preserva histórico (no cubierto aquí)
-- 
-- 5. COLUMNS EXCEPT:
--    - Excluye metadatos de la tabla destino
--    - Mantiene solo datos de negocio
-- 
-- 6. Calidad de datos con CDC:
--    - Validar ANTES de aplicar CDC
--    - Datos malos pueden corromper la tabla destino
-- 
-- 7. Beneficios de AUTO CDC:
--    - No se necesita MERGE manual
--    - Maneja eventos fuera de orden automáticamente
--    - Desduplicación integrada y rendimiento optimizado
-------------------------------------------------------

-------------------------------------------------------
-- EJEMPLO: Cómo funciona AUTO CDC
-------------------------------------------------------
-- Dados estos eventos CDC en orden:
--
-- Evento 1: operation='INSERT'
--   customer_id='C001', name='John Doe', email='john@example.com'
--   Resultado: Nueva fila en la tabla destino
--
-- Evento 2: operation='UPDATE'
--   customer_id='C001', name='John Smith', email='john.smith@example.com'
--   Resultado: Fila existente actualizada
--
-- Evento 3: operation='DELETE'
--   customer_id='C001'
--   Resultado: Fila eliminada de la tabla destino
--
-- La tabla destino refleja siempre el estado ACTUAL
-- No se mantiene histórico (esto es SCD Tipo 1)
-------------------------------------------------------

-------------------------------------------------------
-- MEJORA OPCIONAL: Agregar SCD Tipo 2 para histórico
-------------------------------------------------------
-- Si quieres rastrear histórico, crea un flujo separado:
--
-- CREATE OR REFRESH STREAMING TABLE 2_silver_db.customers_history
--   COMMENT "Histórico de clientes (SCD Tipo 2)";
--
-- CREATE FLOW customers_cdc_flow_scd2 AS 
-- AUTO CDC INTO 2_silver_db.customers_history
-- FROM STREAM bronze.customers_clean
--   KEYS (customer_id)
--   APPLY AS DELETE WHEN operation = 'DELETE'
--   SEQUENCE BY timestamp_datetime
--   COLUMNS * EXCEPT (timestamp, _rescued_data, operation)
--   STORED AS SCD TYPE 2;
--
-- SCD Tipo 2 agrega:
-- - __START_AT: Cuándo se activó esta versión
-- - __END_AT: Cuándo fue reemplazada
-- - __CURRENT: Indica si es la versión actual
-------------------------------------------------------

