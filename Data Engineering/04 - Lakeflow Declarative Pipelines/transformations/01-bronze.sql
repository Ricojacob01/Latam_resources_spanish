-- ==========================================================================
-- BRONZE: Ingesta incremental desde Unity Catalog Volume (Auto Loader)
-- Catálogo: BNS | Esquema: raw | Volumen: transacciones
-- ==========================================================================

-- Transacciones bancarias (CSV)
CREATE OR REFRESH STREAMING TABLE transacciones_raw
COMMENT "Transacciones bancarias en bruto desde archivos CSV."
AS SELECT
  _metadata.file_name AS file_name,
  *
FROM STREAM READ_FILES(
  "/Volumes/${catalog}/${schema}/transacciones/transacciones/*.csv",
  FORMAT => "csv",
  HEADER => true
);

-- Cuentas de clientes (JSON)
CREATE OR REFRESH STREAMING TABLE cuentas_raw
COMMENT "Datos de cuentas bancarias en bruto desde archivos JSON."
AS SELECT
  _metadata.file_name AS file_name,
  *
FROM STREAM READ_FILES(
  "/Volumes/${catalog}/${schema}/transacciones/cuentas/*.json",
  FORMAT => "json"
);

-- Sucursales BNCR (JSON)
CREATE OR REFRESH STREAMING TABLE sucursales_raw
COMMENT "Catálogo de sucursales BNCR en bruto."
AS SELECT
  _metadata.file_name AS file_name,
  *
FROM STREAM READ_FILES(
  "/Volumes/${catalog}/${schema}/transacciones/sucursales/*.json",
  FORMAT => "json"
);

-- Clientes CDC (JSON — eventos INSERT/UPDATE/DELETE)
CREATE OR REFRESH STREAMING TABLE clientes_cdc_raw
COMMENT "Eventos CDC de clientes para procesamiento AUTO CDC."
AS SELECT
  _metadata.file_name AS file_name,
  *
FROM STREAM READ_FILES(
  "/Volumes/${catalog}/${schema}/transacciones/clientes_cdc/*.json",
  FORMAT => "json"
);
