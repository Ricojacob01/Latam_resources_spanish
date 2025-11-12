# ----------------------------------------
# Registrar una UDF de Python como función SQL
# ----------------------------------------

# Este es un cuaderno complementario para cargar el modelo de predicción de turbinas como una UDF de Spark y guardarlo como función SQL
# Asegúrate de añadir este archivo en tu job de Spark Declarative Pipelines para tener acceso a la función SQL `get_turbine_status`.

# Si ejecutas este pipeline en el SDP clásico, puede que necesites poner esto en un notebook y añadir: %pip install mlflow==3.1.0. Ahora usamos entornos en lugar de %pip install.
import mlflow

mlflow.set_registry_uri('databricks-uc')     
predict_maintenance_udf = mlflow.pyfunc.spark_udf(spark, "models:/latam_hunter.dbdemos_iot_turbine.dbdemos_turbine_maintenance@prod", "string", env_manager='virtualenv')
spark.udf.register("predict_maintenance", predict_maintenance_udf)