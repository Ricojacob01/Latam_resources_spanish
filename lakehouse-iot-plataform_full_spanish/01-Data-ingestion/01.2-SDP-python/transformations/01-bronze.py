from pyspark import pipelines as dp

# ----------------------------------
# Ingesta el estado histórico de turbinas desde archivos JSON
# Contiene datos históricos de fallas usados como etiquetas para que el modelo ML identifique turbinas defectuosas
# ----------------------------------
@dp.table(
    name="historical_turbine_status",
    comment="Estado de turbinas usado como etiqueta en nuestro modelo de mantenimiento predictivo (para identificar turbinas potencialmente defectuosas)"
)
@dp.expect("correct_schema", "_rescued_data IS NULL")
def historical_turbine_status():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/latam_hunter/dbdemos_iot_turbine/turbine_raw_landing/historical_turbine_status")
    )



# ----------------------------------
# Ingiera datos de sensores en bruto desde archivos Parquet usando Auto Loader
# Contiene lecturas en tiempo real: vibración (sensores A‑F), energía producida, timestamps, etc.
# Calidad de datos: descarta filas con valores de energía inválidos
# ----------------------------------
@dp.table(
    comment="Datos de sensores en bruto ingeridos incrementalmente con Auto Loader: vibración, energía producida, etc. 1 punto cada X segundos por sensor."
)
@dp.expect("correct_schema", "_rescued_data IS NULL")
@dp.expect_or_drop("correct_energy", "energy IS NOT NULL and energy > 0")
def sensor_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/latam_hunter/dbdemos_iot_turbine/turbine_raw_landing/incoming_data")
    )



# ----------------------------------
# Ingiera metadatos de turbinas desde archivos JSON
# Contiene información estática de la turbina: ubicación (lat/long, estado, país), modelo, ID de turbina
# Estos datos de referencia enriquecen las lecturas con contexto
# ----------------------------------
@dp.table(
    name="turbine",
    comment="Detalles de turbina, con ubicación, tipo de modelo, etc."
)
@dp.expect("correct_schema", "_rescued_data IS NULL")
def turbine():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/latam_hunter/dbdemos_iot_turbine/turbine_raw_landing/turbine")
    )
