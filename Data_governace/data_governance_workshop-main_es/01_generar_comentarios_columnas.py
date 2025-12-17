# Databricks notebook source
# MAGIC %md
# MAGIC # Generación automática de comentarios de columnas con AI_GEN
# MAGIC
# MAGIC Este notebook demuestra cómo utilizar la función **`AI_GEN`** de Databricks para **generar automáticamente descripciones y comentarios de columnas** en tablas del Unity Catalog.  
# MAGIC El objetivo es **acelerar el proceso de documentación de datos**, garantizando mayor claridad y estandarización en los catálogos.
# MAGIC
# MAGIC
# MAGIC ![](./imagens/img_comentarios.png)
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Importante
# MAGIC - Las descripciones y comentarios generados aquí son **solo ejemplos** producidos por IA.  
# MAGIC - Los resultados deben ser **revisados y adaptados** por el usuario antes de aplicarse en producción.  
# MAGIC
# MAGIC > **Atención**: Los comentarios generados automáticamente deben considerarse un **punto de partida**. La curaduría manual es esencial para garantizar calidad y cumplimiento.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición del catálogo y esquema donde se aplicarán los comentarios para todas las tablas

# COMMAND ----------

catalog_name = "governance_demo"
schema_name = "digital_bank"

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrame con todas las tablas, columnas y tipos de dato

# COMMAND ----------

columns_query = f"""
SELECT table_name, column_name, data_type
FROM system.information_schema.columns
WHERE table_catalog = '{catalog_name}' AND table_schema = '{schema_name}'
"""

columns_df = spark.sql(columns_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definiendo la función que genera comentarios para cada columna mediante `AI_GEN`

# COMMAND ----------

def generate_column_comment(row, catalog_name, schema_name):

    prompt = f'''
    Descreva de forma objetiva a finalidade da coluna {row.column_name} na tabela {row.table_name}, considerando o contexto de um banco digital.
    Tipo de dado: {row.data_type}.
    A descrição deve ser clara, útil para analistas de dados e limitada a no máximo 256 caracteres.
    Não repita o nome da coluna ou tabela, não mencione o tipo de dado e não utilize aspas.
    Inclua apenas o comentário, sem explicações adicionais, mas inclua [Gerado por IA] no início de cada comentário.
    '''

    result_df = spark.sql(f"SELECT ai_gen('{prompt}') AS comment")
    column_comment = result_df.collect()[0]['comment']
    return column_comment

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplicando la función y escribiendo los comentarios generados en cada columna

# COMMAND ----------

from pyspark.sql import Row

results = []

for row in columns_df.collect():
    column_comment = generate_column_comment(row, catalog_name, schema_name)
    sql_stmt = f"""
    ALTER TABLE {catalog_name}.{schema_name}.{row.table_name}
    ALTER COLUMN {row.column_name} COMMENT '{column_comment}'
    """
    
    spark.sql(sql_stmt)

    results.append(Row(
        table=row.table_name,
        column=row.column_name,
        comment=column_comment,
        sql=sql_stmt.strip()
    ))

result_df = spark.createDataFrame(results)

display(result_df)
