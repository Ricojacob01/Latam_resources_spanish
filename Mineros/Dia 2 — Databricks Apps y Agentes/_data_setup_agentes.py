# Databricks notebook source
# MAGIC %md
# MAGIC ![](../_recursos/imagenes_agentes/header-genai.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Hands-On LAB 01 - Importando los datos
# MAGIC
# MAGIC Entrenamiento Hands-on en la plataforma de Databricks con foco en las funcionalidades de IA Generativa.
# MAGIC </br></br>
# MAGIC
# MAGIC ## Objetivos del Ejercicio
# MAGIC
# MAGIC El objetivo de este laboratorio es importar los datos que serán utilizados en los próximos ejercicios.
# MAGIC
# MAGIC
# MAGIC

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
# MAGIC ## Ejercicio 01 - Creación de la base de datos
# MAGIC
# MAGIC Primero, vamos crear una base de datos (o esquema; estos nombres se usan como sinónimos). Esta funcionará como un contenedor para guardar los datos que utilizaremos durante los ejercicios.
# MAGIC
# MAGIC **Aislamiento por usuario:** trabajamos en el catálogo compartido `academia` y en **tu propio
# MAGIC esquema** `academia.<tu_apellido>` — el mismo que usaste en el Día 1. Así cada participante
# MAGIC tiene sus propias tablas (`opiniones`, `clientes`, `productos`) sin pisar las de los demás.

# COMMAND ----------

import re

# Catálogo compartido + esquema propio del usuario (igual que en el Día 1)
current_user = spark.sql("SELECT current_user()").collect()[0][0]
clean_username = re.sub(r'[^a-z0-9]', '_', current_user.split("@")[0].lower())

catalog = "academia"
schema = clean_username        # tu esquema personal
volume = "archivos"

# Crear tu esquema y volumen (idempotente)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")

path_volume = "/Volumes/" + catalog + "/" + schema + "/" + volume
path_table = catalog + "." + schema

print(f"✓ Esquema: {catalog}.{schema}")
print(f"✓ Volumen: {path_volume}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ejercicio 02.A - Importando los archivos por código
# MAGIC
# MAGIC Ahora necesitamos cargar los datos que usaremos en los próximos laboratorios.
# MAGIC
# MAGIC Este conjunto consiste básicamente en tres tablas:
# MAGIC - **Opiniones:** contenido de las opiniones
# MAGIC - **Clientes:** datos y consumo de los clientes
# MAGIC - **Produtos:** datos y descripciones de los productos
# MAGIC
# MAGIC Seguid los pasos a continuación para cargar **todas las tablas**: 

# COMMAND ----------

# MAGIC %md
# MAGIC Cargando la tabla con información de **Opiniones**:

# COMMAND ----------

# DBTITLE 1,Cargar la tabla opiniones al volumen
download_url = "https://raw.githubusercontent.com/aestaire/databricks-genai-lab/refs/heads/main/datos/opiniones.csv"
file_name = "opiniones.csv"
table_name = "opiniones"

# Copy the CSV file from the URL to the volume
dbutils.fs.cp(f"{download_url}", f"{path_volume}" + "/" + f"{file_name}")

# COMMAND ----------

# DBTITLE 1,Guardar opiniones como una tabla delta
df = spark.read.csv(f"{path_volume}/{file_name}",
  header=True,
  inferSchema=True,
  sep=",",
  encoding="UTF-8")
display(df)

df.write.mode("overwrite").saveAsTable(f"{path_table}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Cargando la tabla con información de **Clientes**:

# COMMAND ----------

# DBTITLE 1,Cargar la tabla clientes al volumen
download_url = "https://raw.githubusercontent.com/aestaire/databricks-genai-lab/refs/heads/main/datos/clientes.csv"
file_name = "clientes.csv"
table_name = "clientes"

# Copy the CSV file from the URL to the volume
dbutils.fs.cp(f"{download_url}", f"{path_volume}" + "/" + f"{file_name}")


# COMMAND ----------

# DBTITLE 1,Guardar clientes como una tabla delta
df = spark.read.csv(f"{path_volume}/{file_name}",
  header=True,
  inferSchema=True,
  sep=",",
  encoding="UTF-8")
display(df)

df.write.mode("overwrite").saveAsTable(f"{path_table}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Cargando la tabla con información de **Productos**:

# COMMAND ----------

# DBTITLE 1,Cargar la tabla productos al volumen
download_url = "https://raw.githubusercontent.com/aestaire/databricks-genai-lab/refs/heads/main/datos/productos.csv"
file_name = "productos.csv"
table_name = "productos"

# Copy the CSV file from the URL to the volume
dbutils.fs.cp(f"{download_url}", f"{path_volume}" + "/" + f"{file_name}")

# COMMAND ----------

# DBTITLE 1,Guardar productos como una tabla delta
df = spark.read.csv(f"{path_volume}/{file_name}",
  header=True,
  inferSchema=True,
  sep=",",
  encoding="UTF-8")
display(df)

df.write.mode("overwrite").saveAsTable(f"{path_table}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC **IMPORTANTE: Realiza el siguiente ejercicio 02.B únicamente si experimentaste algún problema al cargar los datos. Si todo funcionó correctamente, puedes continuar directamente con el siguiente laboratorio!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 02.B - Importando los archivos manualmente al volúmen y después creando las tablas delta por código
# MAGIC
# MAGIC Ahora necesitamos cargar los datos que usaremos en los próximos laboratorios.
# MAGIC
# MAGIC Este conjunto consiste básicamente en tres tablas:
# MAGIC - **Opiniones:** contenido de las opiniones
# MAGIC - **Clientes:** datos y consumo de los clientes
# MAGIC - **Produtos:** datos y descripciones de los productos
# MAGIC
# MAGIC Seguid los pasos a continuación para cargar **todas las tablas**: 

# COMMAND ----------

# MAGIC %md
# MAGIC 1. En el repositorio de GitHub, en la carpeta ../datos, haz clic en el nombre de cada archivo de datos (csv) y luego haz clic en el botón de descarga del archivo, como se muestra en la figura a continuación:

# COMMAND ----------

# MAGIC %md
# MAGIC ![](../_recursos/imagenes_agentes/descargar-archivos.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. En el menú de Unity Catalog de Databricks, busca tu esquema `academia.<tu_apellido>` y dentro del volúmen "archivos", haz clic en **Upload to this volumen**, como se muestra en la imagen y añade los 3 archivos csv que hemos descargado.

# COMMAND ----------

# MAGIC %md
# MAGIC ![](../_recursos/imagenes_agentes/subir-archivos.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Una vez tenemos los archivos en el volúmen vamos a guardarlos como tablas delta:

# COMMAND ----------

# DBTITLE 1,Guardar opiniones como una tabla delta
file_name = "opiniones.csv"
table_name = "opiniones"
df = spark.read.csv(f"{path_volume}/{file_name}",
  header=True,
  inferSchema=True,
  sep=",",
  encoding="UTF-8")
display(df)

df.write.mode("overwrite").saveAsTable(f"{path_table}.{table_name}")

# COMMAND ----------

# DBTITLE 1,Guardar clientes como una tabla delta
file_name = "clientes.csv"
table_name = "clientes"
df = spark.read.csv(f"{path_volume}/{file_name}",
  header=True,
  inferSchema=True,
  sep=",",
  encoding="UTF-8")
display(df)

df.write.mode("overwrite").saveAsTable(f"{path_table}.{table_name}")

# COMMAND ----------

# DBTITLE 1,Guardar productos como una tabla delta
file_name = "productos.csv"
table_name = "productos"
df = spark.read.csv(f"{path_volume}/{file_name}",
  header=True,
  inferSchema=True,
  sep=",",
  encoding="UTF-8")
display(df)

df.write.mode("overwrite").saveAsTable(f"{path_table}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###¡Ya tenemos todos los datos cargados! ¡Comencemos a crear nuestro primer agente!
# MAGIC
# MAGIC [Lab 02 - Creando un Agente]($./Lab 02 - Creando un Agente)