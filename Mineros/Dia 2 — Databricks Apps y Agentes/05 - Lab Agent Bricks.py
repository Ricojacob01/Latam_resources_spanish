# Databricks notebook source
# MAGIC %md
# MAGIC ![](../_recursos/imagenes_agentes/header-genai.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Bricks: Creando un Knowledge Assistant
# MAGIC
# MAGIC Agent Bricks en Databricks te permite construir asistentes inteligentes que acceden y analizan información relevante de tus datos empresariales. Con Agent Bricks, puedes crear Knowledge Assistants capaces de responder preguntas, extraer insights y proporcionar análisis contextualizados de manera automática.
# MAGIC
# MAGIC En este notebook aprenderás a:
# MAGIC * Configurar un agente especializado utilizando Agent Bricks en Databricks;
# MAGIC * Integrar documentos para enriquecer el conocimiento del asistente;
# MAGIC * Personalizar el comportamiento del agente para responder consultas específicas y generar análisis.
# MAGIC
# MAGIC Esta funcionalidad te ayuda a transformar la información en conocimiento accionable, facilitando la toma de decisiones y mejorando la eficiencia en el acceso a datos clave.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparación
# MAGIC
# MAGIC Para ejecutar los ejercicios, necesitamos conectar este notebook a un clúster/cómputo.
# MAGIC
# MAGIC Simplemente siga los pasos a continuación:
# MAGIC 1. En la esquina superior derecha, haga clic en **Connect**
# MAGIC 2. Seleccione el clúster: **Serverless**

# COMMAND ----------

# MAGIC %md
# MAGIC ![](../_recursos/imagenes_agentes/serverless.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración — tu esquema
# MAGIC Reutilizamos **tu propio esquema** `academia.<tu_apellido>` (el mismo del Día 1 y de los labs
# MAGIC anteriores). Guardaremos el PDF en tu volumen `archivos`.

# COMMAND ----------

import re

current_user = spark.sql("SELECT current_user()").collect()[0][0]
clean_username = re.sub(r'[^a-z0-9]', '_', current_user.split("@")[0].lower())

catalog = "academia"
schema = clean_username        # tu esquema personal
volume = "archivos"

# Crear tu esquema y volumen si aún no existen (idempotente)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")
print(f"✓ Esquema: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 01.A - Importando los datos por código
# MAGIC En este paso vamos a cargar el PDF que utilizaremos durante el laboratorio de Agent Bricks.
# MAGIC
# MAGIC El documento contiene información relacionada con la economía mundial.

# COMMAND ----------

download_url = "https://raw.githubusercontent.com/aestaire/databricks-genai-lab/refs/heads/main/datos/economia_mundial.pdf"
file_name = "economia_mundial.pdf"
path_volume = "/Volumes/" + catalog + "/" + schema + "/" + volume

print(path_volume) # Show the complete path

# Copy the CSV file from the URL to the volume
dbutils.fs.cp(f"{download_url}", f"{path_volume}" + "/" + f"{file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC **IMPORTANTE: Realiza el siguiente ejercicio 01.B únicamente si experimentaste algún problema al cargar los datos. Si todo funcionó correctamente, puedes continuar directamente con el ejercicio 02!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 01.B - Importando los datos manualmente al volúmen
# MAGIC En este paso vamos a cargar el PDF que utilizaremos durante el laboratorio de Agent Bricks.
# MAGIC
# MAGIC El documento contiene información relacionada con la economía mundial.
# MAGIC
# MAGIC 1. En el repositorio de GitHub, en la carpeta ../datos, haz clic en el nombre del archivo "economia_mundial.pdf" y luego haz clic en el botón de descarga del archivo.
# MAGIC 2. En el menú de Unity Catalog de Databricks, busca tu esquema `academia.<tu_apellido>` y dentro del volúmen "archivos", haz clic en **Upload to this volumen**, y añade el archivo pdf que hemos descargado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 02 - Sigue estos pasos para crear el agente:

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="../_recursos/imagenes_agentes/agents.png" width="300">

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="../_recursos/imagenes_agentes/KA.png" width="500">

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="../_recursos/imagenes_agentes/agentbricks.png" width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC **Name** (añade tu apellido al final para que no choque con el de otros participantes)
# MAGIC
# MAGIC knowledge-assistant-2025-economia-`<tu_apellido>`
# MAGIC
# MAGIC **Description**
# MAGIC
# MAGIC Agente especializado en el informe 'Actualización de Perspectivas de la Economía Mundial'. Proporciona análisis y datos clave sobre la resiliencia de la economía y la incertidumbre persistente.
# MAGIC
# MAGIC **Describe the content**
# MAGIC
# MAGIC Informe sobre los últimos datos de crecimiento, proyecciones de inflación o riesgos económicos.
# MAGIC
# MAGIC **Haz clic en: "Create Agent"**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Playground
# MAGIC
# MAGIC Playground es un espacio interactivo donde puedes probar y explorar el Knowledge Assistant que creamos. Aquí puedes hacer preguntas sobre el informe de economía mundial y recibir respuestas automáticas basadas en los datos y análisis disponibles. Solo tienes que escribir tu consulta y el asistente te ayudará con información relevante de manera sencilla y rápida.

# COMMAND ----------

# MAGIC %md
# MAGIC **Clica en "Open in Playground"**

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="../_recursos/imagenes_agentes/KA2.png" width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preguntas
# MAGIC
# MAGIC * ¿Cuáles son las proyecciones económicas para Argentina en 2026?
# MAGIC * ¿Y para México?
# MAGIC * ¿Cuáles son las políticas para restablecer la confianza y garantizar la sostenibilidad?

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¡Felicidades! ¡Has completado tu primer laboratorio de Agentes, AI functions y Agent bricks!
# MAGIC
# MAGIC ¡Ahora ya sabes cómo utilizar Unity Catalog, entrenar y registrar modelos con MLflow, usar las AI functions y crear un agente de knowledge assitant utilizando Agent Bricks!
