# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Título
# MAGIC %md
# MAGIC # 04 - Lakeflow Declarative Pipelines: Arquitectura Medallón
# MAGIC
# MAGIC > **Prerequisito:** Ejecutar `00_setup` completo antes de este lab.

# COMMAND ----------

# DBTITLE 1,Cargar variables
# MAGIC %run "./00 - Setup/00_variables"

# COMMAND ----------

# DBTITLE 1,Verificar entorno
# MAGIC %md
# MAGIC ## A. Verificar entorno
# MAGIC
# MAGIC Seleccione **Serverless** como compute.

# COMMAND ----------

# DBTITLE 1,Validar setup
# Verificar que el setup ya se ejecutó
try:
    spark.sql(f"DESCRIBE CATALOG {catalog_name}")
    dbutils.fs.ls(vol_path)
    print("✅ Entorno OK")
except Exception as e:
    raise RuntimeError("Ejecute primero 00_setup completo") from e
print(f"Volumen: {vol_path}")

# COMMAND ----------

# DBTITLE 1,Crear Pipeline
# MAGIC %md
# MAGIC ## B. Crear Pipeline
# MAGIC
# MAGIC 1. **Jobs & Pipelines → Create → ETL Pipeline**
# MAGIC 2. Nombre: `BNCR Medallion Pipeline - <usuario>`
# MAGIC 3. **Pipeline root:** carpeta `04 - Lakeflow Declarative Pipelines`
# MAGIC 4. Agregar SQL de `transformations/`: `01-bronze.sql`, `02-silver.sql`, `03-gold.sql`
# MAGIC 5. **Destination catalog:** su catálogo | **Destination schema:** su `schema_bronze`
# MAGIC 6. **Configuration:** `catalog` = su catálogo, `schema` = su `schema_raw`, `user_suffix` = su usuario
# MAGIC 7. **Compute:** Serverless → **Start**

# COMMAND ----------

# DBTITLE 1,Verificar tablas
# MAGIC %md
# MAGIC ## C. Verificar tablas (después de que el pipeline termine)

# COMMAND ----------

# DBTITLE 1,Mostrar tablas por capa
for label, schema in [("BRONZE", schema_bronze), ("SILVER", schema_silver), ("GOLD", schema_gold)]:
    print(f"=== {label} ({schema}) ===")
    display(spark.sql(f"SHOW TABLES IN {catalog_name}.{schema}"))

# COMMAND ----------

# DBTITLE 1,Consultar silver
display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_silver}.transacciones LIMIT 10"))

# COMMAND ----------

# DBTITLE 1,Consultar gold
display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_gold}.resumen_diario_sucursal ORDER BY monto_total DESC LIMIT 10"))

# COMMAND ----------

# DBTITLE 1,Tour del Pipeline
# MAGIC %md
# MAGIC ## D. Tour del Pipeline
# MAGIC
# MAGIC ### Checklist
# MAGIC - [ ] Vista DAG
# MAGIC - [ ] Data Quality tab
# MAGIC - [ ] Tablas bronze/silver/gold
# MAGIC - [ ] Linaje en Catalog Explorer
# MAGIC - [ ] Rendimiento por tabla

# COMMAND ----------

# DBTITLE 1,Conteo de filas
tables = [(schema_bronze, "transacciones_raw"), (schema_silver, "transacciones"), (schema_gold, "resumen_diario_sucursal"), (schema_gold, "resumen_producto_canal"), (schema_gold, "metricas_clientes")]
for schema, table in tables:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) as n FROM {catalog_name}.{schema}.{table}").collect()[0]["n"]
        print(f"{schema}.{table}: {cnt:,} filas")
    except Exception as ex:
        print(f"{schema}.{table}: pendiente ({ex})")

# COMMAND ----------

# DBTITLE 1,Segunda ejecución
# MAGIC %md
# MAGIC ## E. Segunda ejecución
# MAGIC
# MAGIC 1. Ejecutar `99_incremental.ipynb`
# MAGIC 2. Re-ejecutar el pipeline
# MAGIC 3. Comparar métricas de calidad