# Databricks notebook source
# MAGIC %md
# MAGIC # Día 1 · Lección 6: Crear un espacio Genie sobre tus tablas Gold
# MAGIC
# MAGIC En las lecciones anteriores construiste un pipeline (Bronze → Silver → Gold) con
# MAGIC datos de **pedidos** y **clientes**. Ahora pondremos un **espacio Genie** encima de
# MAGIC esas tablas para responder preguntas de negocio en **lenguaje natural** (español).
# MAGIC
# MAGIC **Objetivos:**
# MAGIC - Crear un espacio Genie desde la UI (recomendado).
# MAGIC - Conectarlo a las tablas `orders_silver`, `customers_silver` y `order_summary_gold`.
# MAGIC - Definir buenas *instructions* para respuestas en español y precisas.
# MAGIC - Probar preguntas, JOINs, benchmarks y calificación de respuestas.
# MAGIC
# MAGIC **Prerrequisito:** Haber completado las Lecciones 1–3 (pipeline con `orders` y `customers`).
# MAGIC
# MAGIC > 💡 Este mismo Genie lo reutilizaremos el **Día 2** dentro de una Databricks App.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 0: Contexto (mismo catálogo que el Setup)
# MAGIC Derivamos el nombre del catálogo igual que en `01 - Setup` para no depender de escribir tu apellido a mano.

# COMMAND ----------

import re

current_user = spark.sql("SELECT current_user()").collect()[0][0]
clean_username = re.sub(r'[^a-z0-9]', '_', current_user.split("@")[0].lower())

CATALOGO = "academia"           # catálogo compartido
ESQUEMA = clean_username         # tu esquema
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

print(f"Catálogo: {CATALOGO}  ·  Esquema: {ESQUEMA}")
print("Tablas que usará Genie:")
print(f"  - {CATALOGO}.{ESQUEMA}.orders_silver")
print(f"  - {CATALOGO}.{ESQUEMA}.customers_silver")
print(f"  - {CATALOGO}.{ESQUEMA}.order_summary_gold")
print(f"  - {CATALOGO}.{ESQUEMA}.customer_summary_gold  (opcional)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Crear el espacio Genie desde la UI (recomendado)
# MAGIC 1. En el menú lateral, abre **Genie** (sección *SQL* / *AI*).
# MAGIC 2. **New space** y ponle un nombre: `Genie Pedidos y Clientes - <TuApellido>`.
# MAGIC 3. **Fuente de datos:** selecciona el catálogo `academia` → tu esquema `<tu_apellido>` y agrega estas tablas:
# MAGIC    - `orders_silver`
# MAGIC    - `customers_silver`
# MAGIC    - `order_summary_gold`
# MAGIC 4. **General instructions** sugeridas (cópialas):
# MAGIC    - "Responde siempre en español."
# MAGIC    - "El negocio es retail: `orders_silver` son pedidos y `customers_silver` son clientes."
# MAGIC    - "Une pedidos con clientes usando la columna `customer_id`."
# MAGIC    - "`order_summary_gold` ya está agregada por día (`order_date`); úsala para tendencias."
# MAGIC    - "Si la pregunta es ambigua, pide una aclaración y sugiere filtros (fecha, ciudad, estado)."
# MAGIC    - "Cuando aporte valor, sugiere una visualización (barras/series) y limita los resultados."
# MAGIC 5. **Guarda** y prueba una primera pregunta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Definir relaciones entre tablas (clave para JOINs)
# MAGIC Genie genera mejores consultas cuando conoce las relaciones. En las *instructions* del
# MAGIC espacio, agrega explícitamente:
# MAGIC
# MAGIC - "`orders_silver.customer_id` se une con `customers_silver.customer_id`."
# MAGIC - "Para análisis geográfico de pedidos, une `orders_silver` con `customers_silver` y agrupa por `city` o `state`."
# MAGIC
# MAGIC La siguiente celda valida que el JOIN funciona (úsala tú, no Genie):

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pedidos enriquecidos con la ciudad/estado del cliente (INNER JOIN)
# MAGIC SELECT
# MAGIC   o.order_id,
# MAGIC   o.order_timestamp,
# MAGIC   c.customer_id,
# MAGIC   c.name,
# MAGIC   c.city,
# MAGIC   c.state
# MAGIC FROM orders_silver o
# MAGIC INNER JOIN customers_silver c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. 6 preguntas de prueba para Genie
# MAGIC Escríbelas tal cual en el chat de Genie:
# MAGIC 1. ¿Cuántos pedidos hay en total?
# MAGIC 2. ¿Cuántos pedidos hubo por día? Muéstrame una serie temporal.
# MAGIC 3. ¿Cuáles son las 5 ciudades con más pedidos? Gráfico de barras.
# MAGIC 4. ¿Cuántos clientes únicos hay por estado?
# MAGIC 5. ¿Qué día tuvo la mayor cantidad de pedidos?
# MAGIC 6. Muéstrame los pedidos del cliente CUST0001.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Consultas SQL de ejemplo (añádelas como *SQL queries / functions* en las Instructions)
# MAGIC Agregar consultas "de referencia" mejora mucho la precisión de Genie.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pregunta que responde: Top 5 ciudades por número de pedidos
# MAGIC -- Guía de uso: úsala cuando pregunten por pedidos por ciudad
# MAGIC SELECT
# MAGIC   c.city,
# MAGIC   COUNT(o.order_id) AS total_pedidos,
# MAGIC   COUNT(DISTINCT o.customer_id) AS clientes_unicos
# MAGIC FROM orders_silver o
# MAGIC JOIN customers_silver c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC GROUP BY c.city
# MAGIC ORDER BY total_pedidos DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pregunta que responde: tendencia diaria de pedidos con ranking del día
# MAGIC -- Usa la tabla Gold ya agregada + una función de ventana
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   total_daily_orders,
# MAGIC   unique_customers,
# MAGIC   RANK() OVER (ORDER BY total_daily_orders DESC) AS ranking_dia
# MAGIC FROM order_summary_gold
# MAGIC ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Benchmarks (para medir la calidad de Genie)
# MAGIC Para cada pregunta, compara la respuesta de Genie contra el *ground truth* SQL.
# MAGIC Deja una pregunta **sin** ground truth para validarla manualmente.
# MAGIC
# MAGIC | # | Pregunta | Ground Truth (esperado) |
# MAGIC |---|----------|--------------------------|
# MAGIC | 1 | Total de pedidos | `SELECT COUNT(*) FROM orders_silver` |
# MAGIC | 2 | Pedidos por día | `SELECT order_date, total_daily_orders FROM order_summary_gold ORDER BY order_date` |
# MAGIC | 3 | Top 5 ciudades por pedidos | JOIN orders↔customers, GROUP BY city, ORDER BY count DESC LIMIT 5 |
# MAGIC | 4 | Clientes únicos por estado | `SELECT state, COUNT(DISTINCT customer_id) FROM customers_silver GROUP BY state` |
# MAGIC | 5 | Día pico de pedidos | `SELECT order_date FROM order_summary_gold ORDER BY total_daily_orders DESC LIMIT 1` |
# MAGIC
# MAGIC Criterios de aceptación: columnas y tipos razonables, filtros correctos, orden/agrupación coherente, y que sugiera gráfico cuando aplique.

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Descripción del espacio + calificación y monitoreo
# MAGIC 1. En tu espacio Genie, edita **Description / Instructions**:
# MAGIC    - Contexto: "Asistente sobre pedidos y clientes de un negocio retail."
# MAGIC    - Idioma: "Responde en español."
# MAGIC    - Alcance: "No inventes datos fuera de las tablas configuradas."
# MAGIC 2. Usa 👍/👎 en las respuestas y agrega comentarios (exactitud/claridad).
# MAGIC 3. Repite con los Benchmarks para generar señales de calidad.
# MAGIC 4. Revisa el **Monitoring** del espacio: tasa de éxito, tiempos, preguntas frecuentes.
# MAGIC
# MAGIC ---
# MAGIC ### ✅ Cierre de la Lección 6
# MAGIC Ya tienes un Genie funcional sobre tus tablas Gold. **Anota el `Genie Space ID`**
# MAGIC (está en la URL del espacio o en *Settings*): lo necesitarás el **Día 2** para
# MAGIC incrustar el chat de Genie dentro de una Databricks App.

