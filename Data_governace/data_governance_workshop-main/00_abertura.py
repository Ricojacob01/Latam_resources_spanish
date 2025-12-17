# Databricks notebook source
# MAGIC %md
# MAGIC # Potencializando a Governança de Dados com GenAI
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC Este projeto demonstra a implementação de funções de governança de dados utilizando inteligência artificial no ambiente Databricks. O projeto inclui uma série de notebooks que automatizam processos de preparação, classificação e documentação de dados.
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
# MAGIC ## Estrutura do Projeto
# MAGIC
# MAGIC ```
# MAGIC EPL Unity Demo/
# MAGIC ├── _aux_data_preparation.sql              # Notebook SQL - Geração sintética de dados - populando o ambiente
# MAGIC ├── 01_generate_column_comment.py          # Notebook Python - Geração automática de comentários
# MAGIC ├── 02_prompt                              # Notebook Python - Definição do Prompt para classificação de dados 
# MAGIC ├── 03_data_classification.py              # Notebook Python - Classificação automática de dados
# MAGIC ├── 04_results                             # Notebook Python - Comparação com "gabarito"
# MAGIC ```
# MAGIC
# MAGIC ## Como navegar? 
# MAGIC
# MAGIC Com exceção do notebook 00_aux, recomendamos realizar a execução de célula a célula, buscando entender o que cada comando está realizando. O único caderno que exige inserção de comandos é o caderno 02_prompt. Mais instruções podem ser encontradas em cada caderno. 
# MAGIC

# COMMAND ----------

# MAGIC %run ./_aux_data_preparation

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Explorando os dados... 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM governance_demo.digital_bank.cartoes limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM governance_demo.digital_bank.chaves_pix limit 2
