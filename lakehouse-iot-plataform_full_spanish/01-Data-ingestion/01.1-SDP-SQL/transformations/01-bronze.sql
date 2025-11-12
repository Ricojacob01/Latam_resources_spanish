-- ----------------------------------
-- Ingesta el estado histórico de turbinas desde archivos JSON
-- Contiene datos históricos de fallas usados como etiquetas para identificar turbinas defectuosas
-- ----------------------------------
CREATE STREAMING TABLE historical_turbine_status (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Estado de turbina usado como etiqueta en el modelo de mantenimiento predictivo (para identificar turbinas potencialmente defectuosas)"
AS SELECT
  *
FROM STREAM READ_FILES(
    "/Volumes/latam_hunter/dbdemos_iot_turbine/turbine_raw_landing/historical_turbine_status",
    format => "json",
    inferColumnTypes => true
);


-- ----------------------------------
-- Ingiera datos de sensores en bruto desde archivos Parquet usando Auto Loader
-- Contiene lecturas en tiempo real: vibración (sensores A‑F), energía producida, timestamps, etc.
-- Calidad de datos: descarta filas con energía inválida
-- ----------------------------------
CREATE STREAMING TABLE sensor_bronze (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL),
  CONSTRAINT correct_energy EXPECT (energy IS NOT NULL and energy > 0) ON VIOLATION DROP ROW
)
COMMENT "Datos de sensores en bruto ingeridos incrementalmente con Auto Loader: vibración, energía producida, etc. 1 punto cada X segundos por sensor."
AS SELECT
  *
FROM STREAM READ_FILES(
    "/Volumes/latam_hunter/dbdemos_iot_turbine/turbine_raw_landing/incoming_data",
    format => "parquet",
    inferColumnTypes => true);

-- ----------------------------------
-- Ingiera metadatos de turbinas desde archivos JSON
-- Contiene información estática: ubicación (lat/long, estado, país), tipo de modelo, ID de turbina
-- Estos datos de referencia enriquecen las lecturas con contexto
-- ----------------------------------
CREATE STREAMING TABLE turbine (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Detalles de turbina, con ubicación, tipo de modelo, etc."
AS SELECT
  *
FROM STREAM READ_FILES(
    "/Volumes/latam_hunter/dbdemos_iot_turbine/turbine_raw_landing/turbine",
    format => "json",
    inferColumnTypes => true
);