from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ----------------------------------
# Crea tabla de características enriqueciendo métricas horarias con metadatos de turbina
# Selecciona las métricas más recientes por turbina y une con información de ubicación/modelo
# Crea un conjunto de características listo para inferencia de modelos ML
# ----------------------------------
@dp.materialized_view(
    name="turbine_current_features",
    comment="Características de turbina eólica basadas en la predicción del modelo"
)
def turbine_current_features():
    latest_metrics = (
        spark.read.table("sensor_hourly")
        .withColumn(
            "row_number",
            F.row_number().over(
                Window.partitionBy("turbine_id", "hourly_timestamp").orderBy(F.col("hourly_timestamp").desc())
            )
        )
    )
    turbine = spark.read.table("turbine")
    joined = (
        latest_metrics.alias("m")
        .join(turbine.alias("t"), on="turbine_id", how="inner")
        .where((F.col("m.row_number") == 1) & (F.col("turbine_id").isNotNull()))
        .drop("row_number", "_rescued_data", "percentiles_sensor_A", "percentiles_sensor_B", 
              "percentiles_sensor_C", "percentiles_sensor_D", "percentiles_sensor_E", "percentiles_sensor_F")
    )
    return joined


import mlflow

# Carga el modelo ML desde el registro de Unity Catalog y regístralo como UDF
mlflow.set_registry_uri('databricks-uc')
predict_maintenance_udf = mlflow.pyfunc.spark_udf(spark, "models:/latam_hunter.dbdemos_iot_turbine.dbdemos_turbine_maintenance@prod", "string", env_manager='virtualenv')
spark.udf.register("predict_maintenance", predict_maintenance_udf)


# ----------------------------------
# Aplica el modelo ML para predecir necesidades de mantenimiento de turbinas
# Usa la UDF predict_maintenance (cargada desde el registro MLflow) para puntuar cada turbina
# Identifica qué turbinas podrían fallar y requieren mantenimiento preventivo
# ----------------------------------
@dp.materialized_view(
    name="turbine_current_status",
    comment="Último estado de la turbina eólica según la predicción del modelo"
)
def turbine_current_status():
    df = spark.read.table("turbine_current_features")

    return df.withColumn(
        "prediction",
        F.expr("predict_maintenance(hourly_timestamp, avg_energy, std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F, location, model, state)")
    )



# ----------------------------------
# Crea dataset de entrenamiento ML uniendo métricas de sensores con etiquetas históricas de fallas
# Combina características horarias con periodos de falla conocidos para crear datos etiquetados
# El array sensor_vector está optimizado para entrenamiento de modelos
# ----------------------------------
@dp.materialized_view(
    name="turbine_training_dataset",
    comment="Estadísticas de sensores por hora, usadas para describir señal y detectar anomalías"
)
def turbine_training_dataset():
    sensor_hourly = spark.read.table("sensor_hourly")
    turbine = spark.read.table("turbine")
    historical_turbine_status = spark.read.table("historical_turbine_status")

    joined_df = (
        sensor_hourly
        .join(turbine, sensor_hourly.turbine_id == turbine.turbine_id)
        .join(
            historical_turbine_status,
            (sensor_hourly.turbine_id == historical_turbine_status.turbine_id) &
            (F.from_unixtime(historical_turbine_status.start_time) < sensor_hourly.hourly_timestamp) &
            (F.from_unixtime(historical_turbine_status.end_time) > sensor_hourly.hourly_timestamp)
        )
    ).drop(historical_turbine_status.turbine_id, turbine.turbine_id, "_rescued_data")

    result_df = (
        joined_df
        .withColumn("composite_key", F.concat_ws("-", F.col("turbine_id"), F.col("start_time")))
        .withColumn(
            "sensor_vector",
            F.array(
                "std_sensor_A", "std_sensor_B", "std_sensor_C",
                "std_sensor_D", "std_sensor_E", "std_sensor_F"
            )
        )
    )
    return result_df