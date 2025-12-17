# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook de Definição do Prompt
# MAGIC
# MAGIC ## Visão Geral
# MAGIC Esse arquivo serve para armazenarmos e definirmos o prompt que será utilizado em nosso agente de classificação.
# MAGIC
# MAGIC O objetivo é escrever o que você deseje que o prompt classifique. No nosso caso, queremos que ele seja capaz de identificar três classes: **SENSIVEL, CONFIDENCIAL e PUBLICO.**
# MAGIC
# MAGIC Seja o mais claro possível na sua instrução. 
# MAGIC
# MAGIC ## Exemplo
# MAGIC
# MAGIC Abaixo, exemplo de um prompt simples para classificação de acidentes de trabalho em 'LEVE', 'MODERADO' e 'GRAVE'.
# MAGIC
# MAGIC ------------------------
# MAGIC
# MAGIC _Atue como um agente classificação do acidente de trabalho descrito no texto que você receberá em uma das três categorias: LEVE, MODERADO ou GRAVE._
# MAGIC
# MAGIC _LEVE: acidentes sem afastamento ou com ferimentos superficiais._
# MAGIC
# MAGIC _MODERADO: acidentes que exigem cuidados médicos e afastamento temporário._
# MAGIC
# MAGIC _GRAVE: acidentes com risco à vida, sequelas permanentes ou morte.
# MAGIC Responda somente com uma das três categorias: LEVE, MODERADO ou GRAVE._
# MAGIC
# MAGIC -----------------------
# MAGIC
# MAGIC Altere o texto `INSERIR TEXTO AQUI` na célula abaixo. 

# COMMAND ----------

prompt = """
INSERIR TEXTO AQUI 
"""
