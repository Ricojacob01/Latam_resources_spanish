# Databricks notebook source
# MAGIC %md
# MAGIC # Geração Automática de Comentários de Colunas com AI_GEN
# MAGIC
# MAGIC Este notebook demonstra como utilizar a função **`AI_GEN`** do Databricks para **gerar automaticamente descrições e comentários de colunas** em tabelas do Unity Catalog.  
# MAGIC O objetivo é **acelerar o processo de documentação de dados**, garantindo maior clareza e padronização nos catálogos.
# MAGIC
# MAGIC
# MAGIC ![](./imagens/img_comentarios.png)
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Importante
# MAGIC - As descrições e comentários gerados aqui são **apenas exemplos** produzidos por inteligência artificial.  
# MAGIC - Os resultados devem ser **revisados e adaptados** pelo usuário antes de serem aplicados em um ambiente produtivo.  
# MAGIC
# MAGIC > **Atenção**: Os comentários gerados automaticamente devem ser considerados um **ponto de partida**. A curadoria manual é essencial para garantir qualidade e conformidade.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definição do Catálogo e Schema onde serão aplicados os comentários para todas as tabelas

# COMMAND ----------

catalog_name = "governance_demo"
schema_name = "digital_bank"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe contendo todas as tabelas, colunas e data types

# COMMAND ----------

columns_query = f"""
SELECT table_name, column_name, data_type
FROM system.information_schema.columns
WHERE table_catalog = '{catalog_name}' AND table_schema = '{schema_name}'
"""

columns_df = spark.sql(columns_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definindo a função que gera comentários para cada coluna através da `AI_GEN`

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
# MAGIC ### Aplicando a função e implementando a escrita dos comentários gerados em cada coluna

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
