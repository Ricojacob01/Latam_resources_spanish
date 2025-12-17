# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook de Clasificación y Análisis de Datos - Nivel Schema
# MAGIC
# MAGIC ## Visión general
# MAGIC Este notebook procesa y clasifica **todas las tablas** de un esquema con base en los estándares de la LGPD (Ley General de Protección de Datos).
# MAGIC
# MAGIC
# MAGIC ![](./imagens/img_tags_mask.png)
# MAGIC
# MAGIC
# MAGIC ## Estructura modular:
# MAGIC 1. **Informe de Clasificación** - Análisis y clasificación de todos los campos
# MAGIC 2. **Aplicación de Tags** - Etiquetado basado en la clasificación
# MAGIC 3. **Aplicación de Enmascaramiento** - Protección de datos sensibles
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración inicial

# COMMAND ----------

dbutils.widgets.text("catalog_name", "governance_demo", "Nombre del Catálogo")
dbutils.widgets.text("schema_name", "digital_bank", "Nombre del Esquema")
dbutils.widgets.dropdown("apply_tags", "true", ["true", "false"], "Aplicar Tags")
dbutils.widgets.dropdown("apply_mask", "true", ["true", "false"], "Aplicar Enmascaramiento")

# COMMAND ----------

# MAGIC %run ./02_definir_prompt

# COMMAND ----------

try: 
  dbutils.widgets.remove("prompt")
except:
  print('Widget not created')

dbutils.widgets.text("prompt", prompt, "Prompt")

# COMMAND ----------

from pyspark.sql.functions import col, lit
from pyspark.sql import Row
import json

# Get parameter values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
apply_tags = dbutils.widgets.get("apply_tags")
apply_mask = dbutils.widgets.get("apply_mask")

print(f"Processando schema: {catalog_name}.{schema_name}")
print(f"Aplicar tags: {apply_tags}")
print(f"Aplicar mascaramento: {apply_mask}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. INFORME - Clasificación de datos
# MAGIC
# MAGIC Analiza todas las tablas del esquema y clasifica cada campo en:
# MAGIC - **SENSIVEL** - Datos personales sensibles que requieren protección especial
# MAGIC - **CONFIDENCIAL** - Datos confidenciales con acceso restringido
# MAGIC - **PUBLICO** - Datos públicos sin restricciones específicas de privacidad

# COMMAND ----------

# Get all tables in the schema
tables_query = f"""
SELECT table_name, column_name, data_type
FROM system.information_schema.columns
WHERE table_catalog = '{catalog_name}' AND table_schema = '{schema_name}'
"""

tables_df = spark.sql(tables_query)
tables_list = list(set([row.table_name for row in tables_df.collect()]))

print(f"Se encontraron {len(tables_list)} tablas en el esquema {schema_name}")
print(f"Tablas: {', '.join(tables_list)}")

# COMMAND ----------

tables_df.createOrReplaceTempView("tables_df")
display(tables_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name
# MAGIC     , column_name
# MAGIC     , data_type
# MAGIC     , ai_query("databricks-meta-llama-3-3-70b-instruct",
# MAGIC
# MAGIC     concat(:prompt,
# MAGIC     "Devuelve un JSON con dos campos: 'classification' y 'reasoning'.
# MAGIC
# MAGIC     IMPORTANTE: Para 'classification', usa SENSIVEL (sin acento), CONFIDENCIAL y PUBLICO (sin acento). 
# MAGIC     Nombre de la columna: ", column_name, 
# MAGIC     ", Nombre de la tabla", table_name,
# MAGIC     ", Tipo de dato:",data_type),
# MAGIC
# MAGIC     responseFormat => '{
# MAGIC       "type": "json_schema",
# MAGIC       "json_schema": {
# MAGIC         "name": "classificacao_dados",
# MAGIC         "schema": {
# MAGIC           "type": "object",
# MAGIC           "properties": {
# MAGIC             "classification": {"type": "string"},
# MAGIC             "reasoning": {"type": "string"}
# MAGIC                       }
# MAGIC       },
# MAGIC       "strict": true
# MAGIC     }
# MAGIC   }'
# MAGIC ) as json_extracted_info
# MAGIC FROM tables_df;

# COMMAND ----------

df_final = _sqldf ##_sqldf é como fica salvo automaticamente o dataframe criado por uma célula SQL em notebooks

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Define the schema
schema = StructType([
    StructField("classification", StringType(), True),
    StructField("reasoning", StringType(), True)
])

# Transform JSON to columns
transformed_df = _sqldf.select(
    "table_name",
    "column_name",
    "data_type",
    from_json(col("json_extracted_info"), schema).alias("data")
).select(
    "table_name",
    "column_name",
    "data_type",
    "data.*"
)

# Display summary statistics
print("\n=== INFORME DE CLASIFICACIÓN ===")
print(f"Total de columnas clasificadas: {transformed_df.count()}")

# Group by classification"
classification_summary = transformed_df.groupBy("classification").count().orderBy("count", ascending=False)
print("\nResumen por clasificación:")
display(classification_summary)

## Display full df
display(transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. APLICACIÓN DE TAGS
# MAGIC
# MAGIC Aplica tags de clasificación en todas las columnas basándose en el informe generado

# COMMAND ----------

if apply_tags == "true":
   print("=== APLICANDO TAGS DE CLASIFICACIÓN ===\n")
   
   tags_applied = []
   tags_failed = []
   
   for row in transformed_df.collect():
       table_name = row.table_name
       column_name = row.column_name
       classification = row.classification
       
       full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
       
       try:
           alter_sql = f"ALTER TABLE {full_table_name} ALTER COLUMN {column_name} SET TAGS ('category' = '{classification}')"
           print(f"Executing: {alter_sql}")
           spark.sql(alter_sql)
           tags_applied.append(f"{table_name}.{column_name}")
           
       except Exception as e:
           error_msg = str(e)
           tags_failed.append(f"{table_name}.{column_name}: {error_msg}")
           print(f"Error: {error_msg}")
   
   print(f"\n=== RESUMEN DE TAGS ===")
   print(f"Tags aplicadas con éxito: {len(tags_applied)}")
   print(f"Tags con error: {len(tags_failed)}")
       
else:
   print("Aplicación de tags deshabilitada (apply_tags = false)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. APLICACIÓN DE ENMASCARAMIENTO
# MAGIC
# MAGIC Crea funciones de enmascaramiento y las aplica en las columnas SENSIVEL y CONFIDENCIAL

# COMMAND ----------

# Create masking functions if they don't exist
if apply_mask == "true":
    print("=== CREANDO FUNCIONES DE ENMASCARAMIENTO ===\n")
    
    # Función para datos SENSIVEL
    mask_sensivel_sql = f"""
    CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.mask_sensivel_data(column_value STRING) 
    RETURNS STRING
    RETURN 
      CASE 
        WHEN is_member('{schema_name}_users') THEN column_value
        ELSE '***DADOS_SENSIVEIS***'
      END
    """
    
    # Función para datos CONFIDENCIAL  
    mask_confidencial_sql = f"""
    CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.mask_confidencial_data(column_value STRING) 
    RETURNS STRING
    RETURN 
      CASE 
        WHEN is_member('{schema_name}_users') THEN column_value
        ELSE '***DADOS_CONFIDENCIAIS***'
      END
    """
    
    try:
        spark.sql(mask_sensivel_sql)
        print("✓ Función mask_sensivel_data creada")
    except Exception as e:
        print(f"✗ Error al crear función mask_sensivel_data: {str(e)}")
    
    try:
        spark.sql(mask_confidencial_sql)
        print("✓ Función mask_confidencial_data creada")
    except Exception as e:
        print(f"✗ Error al crear función mask_confidencial_data: {str(e)}")

# COMMAND ----------

# Apply masking to columns based on classification
if apply_mask == "true":
    print("\n=== APLICANDO ENMASCARAMIENTO EN COLUMNAS ===\n")
    
    mask_applied = []
    mask_failed = []
    
    # Filtrar columnas sensibles y confidenciales
    sensitive_columns = transformed_df.filter(col("classification") == "SENSIVEL")
    confidential_columns = transformed_df.filter(col("classification") == "CONFIDENCIAL")
    
    # Aplicar máscaras a columnas SENSIVEL
    for row in sensitive_columns.collect():
        table_name = row.table_name
        column_name = row.column_name
        
        try:
            mask_sql = f"""
            ALTER TABLE {catalog_name}.{schema_name}.{table_name} 
            ALTER COLUMN {column_name} 
            SET MASK {catalog_name}.{schema_name}.mask_sensivel_data
            """
            spark.sql(mask_sql)
            mask_applied.append(f"{table_name}.{column_name} (SENSIVEL)")
            print(f"✓ Enmascaramiento aplicado: {table_name}.{column_name} (SENSIVEL)")
            
        except Exception as e:
            mask_failed.append(f"{table_name}.{column_name}: {str(e)}")
            print(f"✗ Error al enmascarar {table_name}.{column_name}: {str(e)}")
    
    # Aplicar máscaras a columnas CONFIDENCIAL
    for row in confidential_columns.collect():
        table_name = row.table_name
        column_name = row.column_name
        
        try:
            mask_sql = f"""
            ALTER TABLE {catalog_name}.{schema_name}.{table_name} 
            ALTER COLUMN {column_name} 
            SET MASK {catalog_name}.{schema_name}.mask_confidencial_data
            """
            spark.sql(mask_sql)
            mask_applied.append(f"{table_name}.{column_name} (CONFIDENCIAL)")
            print(f"✓ Enmascaramiento aplicado: {table_name}.{column_name} (CONFIDENCIAL)")
            
        except Exception as e:
            mask_failed.append(f"{table_name}.{column_name}: {str(e)}")
            print(f"✗ Error al enmascarar {table_name}.{column_name}: {str(e)}")
    
    # Resumen
    print(f"\n=== RESUMEN DE ENMASCARAMIENTO ===")
    print(f"Columnas enmascaradas con éxito: {len(mask_applied)}")
    print(f"Columnas SENSIVEL enmascaradas: {sensitive_columns.count()}")
    print(f"Columnas CONFIDENCIAL enmascaradas: {confidential_columns.count()}")
    print(f"Errores de enmascaramiento: {len(mask_failed)}")
    
else:
    print("Aplicación de enmascaramiento deshabilitada (apply_mask = false)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen

# COMMAND ----------

print("=" * 50)
print("PROCESAMIENTO CONCLUIDO")
print("=" * 50)
print(f\"\nEsquema procesado: {catalog_name}.{schema_name}\")
print(f\"Tablas analizadas: {len(tables_list)}\")
print(f\"Total de columnas clasificadas: {transformed_df.count()}\")

if apply_tags == "true":
    print(f"\nTags aplicadas: SÍ")
else:
    print(f"\nTags aplicadas: NO")

if apply_mask == "true":
    print(f"Enmascaramiento aplicado: SÍ")
else:
    print(f"Enmascaramiento aplicado: NO")

print("\nClassificação por tipo:")
display(transformed_df.groupBy("classification").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Probando nuevamente...

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM governance_demo.digital_bank.cartoes limit 2

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM governance_demo.digital_bank.chaves_pix limit 2

# COMMAND ----------


