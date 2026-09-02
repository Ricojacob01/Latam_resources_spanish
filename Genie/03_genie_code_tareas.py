# Databricks notebook source
# DBTITLE 1,Genie Code para distintas tareas
# MAGIC %md
# MAGIC # 03 · **Genie Code** para distintas tareas  ·  (no solo explorar)
# MAGIC
# MAGIC En `02` usamos Genie Code para **explorar**. Aquí lo usamos para **todo el ciclo** de construir el agente.
# MAGIC Cada sección tiene: 🧞 **prompts para pegar en Genie Code**, ✍️ **qué capturar**, y una celda de
# MAGIC **verificación opcional**. Todo corre sobre el esquema del taller configurado en `00_config`.
# MAGIC
# MAGIC | # | Tarea con Genie Code | Para qué sirve |
# MAGIC |---|---|---|
# MAGIC | 1 | **Preparar datos** (denormalizar, vistas gold) | Genie es más preciso con tablas anchas y limpias |
# MAGIC | 2 | **Calidad de datos** | Detectar nulos, duplicados, rangos raros antes de exponer |
# MAGIC | 3 | **Construir métricas** | Traducir definiciones de negocio a SQL certificado |
# MAGIC | 4 | **Descubrir sinónimos y valores** | Mapear términos del usuario a los valores reales |
# MAGIC | 5 | **Generar sample questions** | Preguntas semilla del agente |
# MAGIC | 6 | **Generar benchmarks** | Batería de pruebas con SQL esperado |
# MAGIC | 7 | **Redactar text_instructions** | El texto que hace fiable al agente |
# MAGIC
# MAGIC > 💡 Flujo con Genie Code: pega el prompt → revisa el SQL → ejecútalo → si está bien, **guárdalo**
# MAGIC > (es material del agente); si está mal, **corrige el prompt o anota qué le faltó saber** (es una instrucción).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 🧞 Preparar datos — denormalizar para mejorar la fiabilidad
# MAGIC Genie responde mejor sobre **una tabla ancha** que sobre muchas tablas con joins. Pídele a Genie Code
# MAGIC que construya una vista `gold_` que ya traiga los atributos de las dimensiones.
# MAGIC
# MAGIC **Prompts:**
# MAGIC - Crea una vista llamada `gold_oportunidades` que una `fact_opportunity` con `dim_rep` y `dim_account`
# MAGIC   y traiga: región, segmento, nombre del representante, gerente, industria de la cuenta, monto, etapa,
# MAGIC   `is_open`, `is_won`, fechas y trimestre fiscal.
# MAGIC - Agrégale a esa vista una columna con los días de ciclo de venta (`close_date - created_date`).
# MAGIC - Documenta cada columna de la vista con un comentario claro.
# MAGIC
# MAGIC ✍️ **Captura:** el nombre de la vista denormalizada → será una de las *tables* del agente (mejor que exponer las 4 crudas).

# COMMAND ----------

# (Opcional) Verifica lo que Genie Code haya creado:
# display(spark.sql(f"SHOW VIEWS IN {fq_schema}"))
# display(spark.sql(f"SELECT * FROM {fq_schema}.gold_oportunidades LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 🧞 Calidad de datos — antes de exponer al agente
# MAGIC Un agente sobre datos sucios da respuestas sucias. Usa Genie Code para auditar:
# MAGIC
# MAGIC **Prompts:**
# MAGIC - ¿Qué porcentaje de valores nulos tiene cada columna de `fact_opportunity`?
# MAGIC - ¿Hay `opp_id` duplicados en `fact_opportunity`?
# MAGIC - ¿Hay oportunidades cuyo `close_date` sea anterior al `created_date`? (fechas inválidas)
# MAGIC - ¿Hay `rep_id` en `fact_opportunity` que no existan en `dim_rep`? (integridad referencial)
# MAGIC - ¿El monto (`amount`) tiene valores negativos o atípicos?
# MAGIC
# MAGIC ✍️ **Captura:** cada problema encontrado → o se corrige en la preparación (paso 1), o se documenta como
# MAGIC una *instrucción* ("ignora filas con amount <= 0").

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 🧞 Construir métricas — de definición de negocio a SQL certificado
# MAGIC Toma cada definición del glosario (Día 1) y pídele a Genie Code el SQL. El SQL que valides
# MAGIC se vuelve un **`example_question_sql`** del agente (oro puro para la precisión).
# MAGIC
# MAGIC **Prompts (ejemplo del dominio de ventas — adáptalos al cliente):**
# MAGIC - Calcula el **pipeline abierto** total = suma de `amount` donde `is_open` es verdadero.
# MAGIC - Calcula la **cobertura de pipeline por región** = pipeline abierto ÷ meta (de `fact_region_target`).
# MAGIC - Calcula los **ingresos ganados por línea de producto** = suma de `amount` donde `is_won`.
# MAGIC - Calcula la **tasa de conversión por región** = ganadas ÷ (ganadas + perdidas).
# MAGIC - Calcula el **ciclo de venta promedio en días** de las oportunidades ganadas.
# MAGIC
# MAGIC ✍️ **Captura:** pregunta + SQL validado → pásalo a `benchmarks.csv` y a los `example_question_sqls`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 🧞 Descubrir sinónimos y valores reales
# MAGIC Los usuarios dicen "ganado"; la columna guarda "Cerrada Ganada". Ese *gap* es la causa #1 de respuestas vacías.
# MAGIC
# MAGIC **Prompts:**
# MAGIC - Lista los valores distintos de `stage` con su conteo.
# MAGIC - Lista las regiones distintas y las líneas de producto distintas.
# MAGIC - ¿Qué industrias distintas hay en `dim_account`?
# MAGIC
# MAGIC ✍️ **Captura** un mini-diccionario término-de-usuario → valor-real. Va en las `text_instructions`:
# MAGIC
# MAGIC | Dice el usuario | Valor real en la columna |
# MAGIC |---|---|
# MAGIC | ganado / won | Cerrada Ganada |
# MAGIC | perdido / lost | Cerrada Perdida |
# MAGIC | meta / cuota / target | quota_amount |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 🧞 Generar sample questions (preguntas semilla)
# MAGIC **Prompt:**
# MAGIC - Con base en estas tablas y columnas, propón 8 preguntas de negocio en español que un líder comercial
# MAGIC   haría y que se puedan responder con estos datos. Devuélvelas como lista.
# MAGIC
# MAGIC ✍️ **Captura:** elige 4–6 → serán las `sample_questions` del agente (`config.sample_questions`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 🧞 Generar benchmarks (batería de pruebas)
# MAGIC **Prompt:**
# MAGIC - Genera 15 preguntas de prueba en español sobre estos datos, con el SQL esperado de cada una.
# MAGIC   Marca las 5 más críticas del negocio como "tier-1".
# MAGIC
# MAGIC ✍️ **Captura:** vacíalas en `benchmarks_TEMPLATE.csv` (columnas: id, tier, pregunta, sql_esperado, notas).
# MAGIC Las tier-1 son las que **deben** pasar (meta ≥ 85%).

# COMMAND ----------

# (Opcional) Exporta un CSV de benchmarks vacío en tu esquema como archivo de trabajo:
# import pandas as pd
# plantilla = pd.DataFrame({"id": range(1, 16),
#                           "tier": ["tier-1"]*5 + ["tier-2"]*5 + ["tier-3"]*5,
#                           "pregunta": "", "sql_esperado": "", "notas": ""})
# plantilla.to_csv("/tmp/benchmarks_cliente.csv", index=False)
# print("Plantilla en /tmp/benchmarks_cliente.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 🧞 Redactar las text_instructions del agente
# MAGIC Consolida TODO lo anterior en un solo texto (el agente acepta **una sola** `text_instructions`).
# MAGIC
# MAGIC **Prompt:**
# MAGIC - Redacta instrucciones para un agente Genie de ventas en español que incluyan: (a) definiciones de negocio
# MAGIC   con su fórmula y tabla/columna, (b) dónde está cada métrica, (c) las claves de unión entre tablas,
# MAGIC   (d) los sinónimos término-de-usuario → valor-real, y (e) el periodo por defecto.
# MAGIC
# MAGIC ✍️ **Captura:** ese texto va en `instructions.text_instructions[0].content`.
# MAGIC Mira `genie_agent.json` (ejemplo del dominio ventas) para ver cómo se ve uno bueno.
# MAGIC
# MAGIC ➡️ Con esto listo, genera el agente con **`04_build_genie_agent.py`** y créalo con **`05_create_genie_agent.sh`**.