# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook de Definición del Prompt
# MAGIC
# MAGIC ## Visión general
# MAGIC Este archivo sirve para almacenar y definir el prompt que se utilizará en nuestro agente de clasificación.
# MAGIC
# MAGIC El objetivo es escribir lo que deseas que el prompt clasifique. En nuestro caso, queremos que sea capaz de identificar tres clases: **SENSIVEL, CONFIDENCIAL y PUBLICO.**
# MAGIC
# MAGIC Sé lo más claro posible en tu instrucción. 
# MAGIC
# MAGIC ## Ejemplo
# MAGIC
# MAGIC A continuación, ejemplo de un prompt simple para clasificar accidentes laborales en 'LEVE', 'MODERADO' y 'GRAVE'.
# MAGIC
# MAGIC ------------------------
# MAGIC
# MAGIC _Actúa como un agente de clasificación del accidente de trabajo descrito en el texto que recibirás en una de las tres categorías: LEVE, MODERADO o GRAVE._
# MAGIC
# MAGIC _LEVE: accidentes sin baja o con lesiones superficiales._
# MAGIC
# MAGIC _MODERADO: accidentes que requieren atención médica y baja temporal._
# MAGIC
# MAGIC _GRAVE: accidentes con riesgo para la vida, secuelas permanentes o muerte.
# MAGIC Responde solo con una de las tres categorías: LEVE, MODERADO o GRAVE._
# MAGIC
# MAGIC -----------------------
# MAGIC
# MAGIC Cambia el texto `INSERTAR TEXTO AQUÍ` en la celda de abajo. 

# COMMAND ----------

prompt = """
INSERTAR TEXTO AQUÍ 
"""
