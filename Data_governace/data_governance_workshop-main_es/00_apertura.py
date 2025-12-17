# Databricks notebook source
# MAGIC %md
# MAGIC # Potenciando la Gobernanza de Datos con GenAI
# MAGIC
# MAGIC ## Visión general
# MAGIC
# MAGIC Este proyecto demuestra la implementación de funciones de gobernanza de datos utilizando inteligencia artificial en Databricks. Incluye una serie de notebooks que automatizan procesos de preparación, clasificación y documentación de datos.
# MAGIC
# MAGIC ## Motivação 
# MAGIC
# MAGIC Conforme os ambientes de dados vão crescendo, e a geração de dados passa a ser descentralizada, o controle de quais dados estão sendo salvos no banco torna-se cada vez mais desafiador. Os gerentes de governança tem o desafio de viabilizar as análises e consumo de dados ao mesmo tempo que controla a segurança do ambiente. Rapidamente passa a ser inviável realizar classificações manuais das tabelas. 
# MAGIC
# MAGIC Com IA e criação de políticas, é possível gerenciar acesso e realizar classificação de dados em larga escala!
# MAGIC
# MAGIC ![](./imagens/comparacao_governanca.png)
# MAGIC
# MAGIC
# MAGIC ## 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estructura del proyecto
# MAGIC
# MAGIC ```
# MAGIC EPL Unity Demo/
# MAGIC ├── _aux_data_preparation.sql              # Notebook SQL - Generación sintética de datos - poblando el entorno
# MAGIC ├── 01_generate_column_comment.py          # Notebook Python - Generación automática de comentarios
# MAGIC ├── 02_prompt                              # Notebook Python - Definición del prompt para clasificación de datos 
# MAGIC ├── 03_data_classification.py              # Notebook Python - Clasificación automática de datos
# MAGIC ├── 04_results                             # Notebook Python - Comparación con “gabarito”
# MAGIC ```
# MAGIC
# MAGIC ## ¿Cómo navegar?
# MAGIC
# MAGIC Excepto el notebook auxiliar, recomendamos ejecutar célula por célula para entender qué hace cada comando. El único cuaderno que requiere ingreso de texto es `02_prompt`. Más instrucciones se encuentran en cada cuaderno. 
# MAGIC

# COMMAND ----------

# MAGIC %run ./_aux_preparacion_datos

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Explorando los datos... 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM governance_demo.digital_bank.cartoes limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM governance_demo.digital_bank.chaves_pix limit 2
