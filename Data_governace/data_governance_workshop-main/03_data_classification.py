# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook de Classificação e Análise de Dados - Nível Schema
# MAGIC
# MAGIC ## Visão Geral
# MAGIC Este notebook processa e classifica **todas as tabelas** de um schema com base nos padrões da LGPD (Lei Geral de Proteção de Dados).
# MAGIC
# MAGIC
# MAGIC ![](./imagens/img_tags_mask.png)
# MAGIC
# MAGIC
# MAGIC ## Estrutura Modular:
# MAGIC 1. **Relatório de Classificação** - Análise e classificação de todos os campos
# MAGIC 2. **Aplicação de Tags** - Tagueamento baseado na classificação
# MAGIC 3. **Aplicação de Mascaramento** - Proteção de dados sensíveis
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

dbutils.widgets.text("catalog_name", "governance_demo", "Catalog Name")
dbutils.widgets.text("schema_name", "digital_bank", "Schema Name")
dbutils.widgets.dropdown("apply_tags", "true", ["true", "false"], "Apply Tags")
dbutils.widgets.dropdown("apply_mask", "true", ["true", "false"], "Apply Masking")

# COMMAND ----------

# MAGIC %run ./02_prompt

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
# MAGIC ## 1. RELATÓRIO - Classificação de Dados
# MAGIC
# MAGIC Analisa todas as tabelas do schema e classifica cada campo em:
# MAGIC - **SENSÍVEL** - Dados pessoais sensíveis que requerem proteção especial
# MAGIC - **CONFIDENCIAL** - Dados confidenciais com acesso restrito
# MAGIC - **PÚBLICO** - Dados públicos sem restrições específicas de privacidade

# COMMAND ----------

# Get all tables in the schema
tables_query = f"""
SELECT table_name, column_name, data_type
FROM system.information_schema.columns
WHERE table_catalog = '{catalog_name}' AND table_schema = '{schema_name}'
"""

tables_df = spark.sql(tables_query)
tables_list = list(set([row.table_name for row in tables_df.collect()]))

print(f"Encontradas {len(tables_list)} tabelas no schema {schema_name}")
print(f"Tabelas: {', '.join(tables_list)}")

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
# MAGIC     "Retorne um JSON dois campos: 'classification' e 'reasoning'.
# MAGIC
# MAGIC     IMPORTANTE: Para o classification, use SENSIVEL (sem acento), CONFIDENCIAL e PUBLICO (sem acento). 
# MAGIC     Nome da coluna: ", column_name, 
# MAGIC     ", Nome da tabela", table_name,
# MAGIC     ", Tipo do dado:",data_type),
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
print("\n=== RELATÓRIO DE CLASSIFICAÇÃO ===")
print(f"Total de colunas classificadas: {transformed_df.count()}")

# Group by classification"
classification_summary = transformed_df.groupBy("classification").count().orderBy("count", ascending=False)
print("\nResumo por classificação:")
display(classification_summary)

## Display full df
display(transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. APLICAÇÃO DE TAGS
# MAGIC
# MAGIC Aplica tags de classificação em todas as colunas baseado no relatório gerado

# COMMAND ----------

if apply_tags == "true":
   print("=== APLICANDO TAGS DE CLASSIFICAÇÃO ===\n")
   
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
   
   print(f"\n=== RESUMO DE TAGS ===")
   print(f"Tags aplicadas com sucesso: {len(tags_applied)}")
   print(f"Tags com erro: {len(tags_failed)}")
       
else:
   print("Aplicação de tags desabilitada (apply_tags = false)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. APLICAÇÃO DE MASCARAMENTO
# MAGIC
# MAGIC Cria funções de mascaramento e aplica nas colunas SENSÍVEL e CONFIDENCIAL

# COMMAND ----------

# Create masking functions if they don't exist
if apply_mask == "true":
    print("=== CRIANDO FUNÇÕES DE MASCARAMENTO ===\n")
    
    # Function for SENSÍVEL data
    mask_sensivel_sql = f"""
    CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.mask_sensivel_data(column_value STRING) 
    RETURNS STRING
    RETURN 
      CASE 
        WHEN is_member('{schema_name}_users') THEN column_value
        ELSE '***DADOS_SENSIVEIS***'
      END
    """
    
    # Function for CONFIDENCIAL data  
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
        print("✓ Função mask_sensivel_data criada")
    except Exception as e:
        print(f"✗ Erro ao criar função mask_sensivel_data: {str(e)}")
    
    try:
        spark.sql(mask_confidencial_sql)
        print("✓ Função mask_confidencial_data criada")
    except Exception as e:
        print(f"✗ Erro ao criar função mask_confidencial_data: {str(e)}")

# COMMAND ----------

# Apply masking to columns based on classification
if apply_mask == "true":
    print("\n=== APLICANDO MASCARAMENTO NAS COLUNAS ===\n")
    
    mask_applied = []
    mask_failed = []
    
    # Filter sensitive and confidential columns
    sensitive_columns = transformed_df.filter(col("classification") == "SENSIVEL")
    confidential_columns = transformed_df.filter(col("classification") == "CONFIDENCIAL")
    
    # Apply masks to SENSÍVEL columns
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
            mask_applied.append(f"{table_name}.{column_name} (SENSÍVEL)")
            print(f"✓ Mascaramento aplicado: {table_name}.{column_name} (SENSÍVEL)")
            
        except Exception as e:
            mask_failed.append(f"{table_name}.{column_name}: {str(e)}")
            print(f"✗ Erro ao mascarar {table_name}.{column_name}: {str(e)}")
    
    # Apply masks to CONFIDENCIAL columns
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
            print(f"✓ Mascaramento aplicado: {table_name}.{column_name} (CONFIDENCIAL)")
            
        except Exception as e:
            mask_failed.append(f"{table_name}.{column_name}: {str(e)}")
            print(f"✗ Erro ao mascarar {table_name}.{column_name}: {str(e)}")
    
    # Summary
    print(f"\n=== RESUMO DE MASCARAMENTO ===")
    print(f"Colunas mascaradas com sucesso: {len(mask_applied)}")
    print(f"Colunas SENSÍVEIS mascaradas: {sensitive_columns.count()}")
    print(f"Colunas CONFIDENCIAIS mascaradas: {confidential_columns.count()}")
    print(f"Erros de mascaramento: {len(mask_failed)}")
    
else:
    print("Aplicação de mascaramento desabilitada (apply_mask = false)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

print("=" * 50)
print("PROCESSAMENTO CONCLUÍDO")
print("=" * 50)
print(f"\nSchema processado: {catalog_name}.{schema_name}")
print(f"Tabelas analisadas: {len(tables_list)}")
print(f"Total de colunas classificadas: {transformed_df.count()}")

if apply_tags == "true":
    print(f"\nTags aplicadas: SIM")
else:
    print(f"\nTags aplicadas: NÃO")

if apply_mask == "true":
    print(f"Mascaramento aplicado: SIM")
else:
    print(f"Mascaramento aplicado: NÃO")

print("\nClassificação por tipo:")
display(transformed_df.groupBy("classification").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Testando novamente...

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM governance_demo.digital_bank.cartoes limit 2

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM governance_demo.digital_bank.chaves_pix limit 2

# COMMAND ----------


