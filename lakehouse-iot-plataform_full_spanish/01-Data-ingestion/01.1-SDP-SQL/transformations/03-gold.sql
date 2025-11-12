-- ----------------------------------
-- Crea tabla de características enriqueciendo métricas horarias con metadatos de turbina
-- Selecciona métricas más recientes por turbina y une con ubicación/modelo
-- Crea un conjunto de características listo para inferencia de modelos ML
-- ----------------------------------
-- specify all the field to enforce the primary key
CREATE MATERIALIZED VIEW turbine_current_features
 (
    turbine_id STRING NOT NULL,
    hourly_timestamp TIMESTAMP,
    avg_energy DOUBLE,
    std_sensor_A DOUBLE,
    std_sensor_B DOUBLE,
    std_sensor_C DOUBLE,
    std_sensor_D DOUBLE,
    std_sensor_E DOUBLE,
    std_sensor_F DOUBLE,
    country STRING,
    lat STRING,
    location STRING,
    long STRING,
    model STRING,
    state STRING,
   CONSTRAINT turbine_current_features_pk PRIMARY KEY (turbine_id))
 COMMENT "Características de turbina eólica basadas en la predicción del modelo"
AS
WITH latest_metrics AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY turbine_id, hourly_timestamp ORDER BY hourly_timestamp DESC) AS row_number FROM sensor_hourly
)
SELECT * EXCEPT(m.row_number,_rescued_data, percentiles_sensor_A,percentiles_sensor_B, percentiles_sensor_C, percentiles_sensor_D, percentiles_sensor_E, percentiles_sensor_F)
FROM latest_metrics m
   INNER JOIN turbine t USING (turbine_id)
   WHERE m.row_number=1 and turbine_id is not null;


-- ----------------------------------
-- Aplica el modelo ML para predecir necesidades de mantenimiento de turbinas
-- Usa la UDF predict_maintenance (cargada desde el registro MLflow) para puntuar cada turbina
-- Identifica turbinas propensas a fallar y requerir mantenimiento preventivo
-- ----------------------------------
-- Nota: el modelo predict_maintenance se carga desde el notebook 01.2-DLT-Wind-Turbine-SQL-UDF
CREATE MATERIALIZED VIEW turbine_current_status
COMMENT "Último estado de la turbina eólica según la predicción del modelo"
AS
SELECT *,
    predict_maintenance(hourly_timestamp, avg_energy, std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F, location, model, state) as prediction
  FROM turbine_current_features;

-- ----------------------------------
-- Crea dataset de entrenamiento ML uniendo métricas de sensores con etiquetas de fallas históricas
-- Combina características por hora con periodos de falla conocidos para crear datos etiquetados
-- El array sensor_vector está optimizado para entrenamiento de modelos
-- ----------------------------------
CREATE MATERIALIZED VIEW turbine_training_dataset
COMMENT "Estadísticas de sensores por hora, usadas para describir señal y detectar anomalías"
AS
SELECT CONCAT(t.turbine_id, '-', s.start_time) AS composite_key, array(std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F) AS sensor_vector, * except(t._rescued_data, s._rescued_data, m.turbine_id) FROM sensor_hourly m
    INNER JOIN turbine t USING (turbine_id)
    INNER JOIN historical_turbine_status s ON m.turbine_id = s.turbine_id AND from_unixtime(s.start_time) < m.hourly_timestamp AND from_unixtime(s.end_time) > m.hourly_timestamp;

