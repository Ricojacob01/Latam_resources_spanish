# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Explora con Genie Code
# MAGIC %md
# MAGIC # 02 · Explora CUALQUIER dataset con **Genie Code**  ·  (reutilizable por cliente)
# MAGIC
# MAGIC **Metodología: _Genie Code primero_.** El objetivo del taller es que los usuarios hagan la mayor
# MAGIC parte de la exploración **preguntando en lenguaje natural con Genie Code**, no escribiendo SQL a mano.
# MAGIC
# MAGIC Este notebook:
# MAGIC  1. te da el **contexto mínimo** (esquema y tamaños) usando tu esquema del taller,
# MAGIC  2. te entrega un **catálogo de prompts** para pegar en Genie Code, y
# MAGIC  3. deja las celdas SQL **solo como verificación opcional**.
# MAGIC
# MAGIC > **Cómo usar Genie Code:** abre el panel ✨ *Assistant / Genie* del notebook, pega un prompt,
# MAGIC > revisa el SQL que genera, ejecútalo y evalúa la respuesta. Cada corrida = consumo.
# MAGIC
# MAGIC 🔑 `00_config` (abajo con `%run`) fija el catálogo y esquema compartidos.
# MAGIC Por defecto exploras el esquema del taller; para explorar las tablas reales del cliente,
# MAGIC usa el widget `explorar_schema` (opcional).

# COMMAND ----------

# MAGIC %md ### 0. Configuración común (catálogo compartido + tu esquema)

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Por defecto exploramos TU esquema del taller (schema viene de 00_config).
# Para explorar las tablas REALES del cliente, escribe su esquema en 'explorar_schema'
# y (si es otro catálogo) en 'explorar_catalog'. Vacío = usa tu esquema del taller.
dbutils.widgets.text("explorar_catalog", "", "A. Explorar otro catálogo (opcional)")
dbutils.widgets.text("explorar_schema", "", "B. Explorar otro esquema (opcional)")
dbutils.widgets.text("tables", "", "C. Tablas (CSV, opcional; vacío = todas)")

exp_catalog = dbutils.widgets.get("explorar_catalog").strip() or catalog
exp_schema = dbutils.widgets.get("explorar_schema").strip() or schema
tables_csv = dbutils.widgets.get("tables").strip()
exp_fq = f"`{exp_catalog}`.`{exp_schema}`"
print(f"Explorando: {exp_fq}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Contexto mínimo para Genie Code
# MAGIC Corre esto UNA vez para que todos veamos qué tablas hay. **A partir de aquí, usa Genie Code.**

# COMMAND ----------

if tables_csv:
    tables = [t.strip() for t in tables_csv.split(",") if t.strip()]
else:
    tables = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {exp_fq}").collect() if not r["isTemporary"]]
print(f"{len(tables)} tablas en {exp_fq}: {tables}")
for t in tables:
    print(f"\n===== {t} =====")
    try:
        display(spark.sql(f"DESCRIBE TABLE `{exp_catalog}`.`{exp_schema}`.`{t}`"))
    except Exception as e:
        print(f"[skip] {t}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 🧞 Explora con Genie Code — DESCUBRIMIENTO
# MAGIC Pega estos en el panel de Genie Code (uno por uno). Ajusta nombres al negocio del cliente.
# MAGIC
# MAGIC - ¿Qué tablas hay disponibles y de qué trata cada una?
# MAGIC - ¿Cuántos registros tiene cada tabla?
# MAGIC - Muéstrame 10 filas de ejemplo de la tabla más grande.
# MAGIC - ¿Qué columnas parecen ser identificadores o claves para unir tablas?
# MAGIC - ¿Qué relaciones existen entre estas tablas?
# MAGIC
# MAGIC ✍️ **Anota:** ¿el SQL que generó Genie fue correcto? Si no, ¿qué le faltó saber? (eso se vuelve *instrucción* del agente).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 🧞 Explora con Genie Code — MÉTRICAS DE NEGOCIO
# MAGIC El objetivo del Día 1 es descubrir las métricas clave **a través de Genie Code**:
# MAGIC
# MAGIC - ¿Cuáles son los totales o volúmenes más importantes en estos datos?
# MAGIC - Dame los principales <categoría> por <medida> (ej. "top regiones por monto").
# MAGIC - ¿Cuál es el promedio / máximo / mínimo de <medida>?
# MAGIC - ¿Cómo cambia <medida> a lo largo del tiempo?
# MAGIC - ¿Cómo se distribuye <medida> por <categoría>?
# MAGIC - Compara <categoría A> vs <categoría B> en <medida>.
# MAGIC
# MAGIC ✍️ Las preguntas que Genie responde bien → *sample questions* del agente. Las que falla → *benchmarks* para afinar.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 🧞 Explora con Genie Code — CALIDAD Y SIGNIFICADO
# MAGIC Para definir sinónimos e instrucciones, pregúntale a Genie por los valores reales:
# MAGIC
# MAGIC - ¿Qué valores distintos tiene la columna <estado/categoría> y cuántos registros hay de cada uno?
# MAGIC - ¿Qué porcentaje de <columna> está vacío o nulo?
# MAGIC - ¿Hay duplicados por <clave>?
# MAGIC - ¿Cuál es el rango de fechas de los datos?
# MAGIC
# MAGIC ✍️ Ej.: si el usuario dice "ganado" pero la columna guarda "Closed Won", eso es un **sinónimo** que va en las instrucciones.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. (Opcional) Verificación con SQL
# MAGIC Solo si quieres confirmar lo que Genie Code respondió. **No es el foco** — el foco es Genie Code.
# MAGIC Descomenta y ajusta a una tabla/columna reales.

# COMMAND ----------

# t = tables[0]
# display(spark.sql(f"SELECT COUNT(*) AS filas FROM `{exp_catalog}`.`{exp_schema}`.`{t}`"))
# display(spark.sql(f"SELECT * FROM `{exp_catalog}`.`{exp_schema}`.`{t}` LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salida del Día 1 — Glosario / ontología 
# MAGIC Todo lo aprendido con Genie Code se resume aquí y alimenta las *text_instructions* del agente.
# MAGIC
# MAGIC | Término de negocio | Definición | Tabla / columna | Sinónimos |
# MAGIC |---|---|---|---|
# MAGIC | _(ej. Ingresos)_ | _(SUM(...) WHERE ...)_ | _(tabla.columna)_ | _(sinónimos)_ |
# MAGIC | | | | |
# MAGIC
# MAGIC **Checklist de cierre Día 1:**
# MAGIC - [ ] Prompts de Genie Code que funcionaron → lista de *sample questions*
# MAGIC - [ ] Prompts que fallaron → filas de `benchmarks_TEMPLATE.csv`
# MAGIC - [ ] Glosario con definiciones, claves de unión y sinónimos
# MAGIC - [ ] Consumo visible en el workspace ✅
# MAGIC
# MAGIC ➡️ Sigue con **`03_genie_code_tareas`**: usar Genie Code para preparar datos, calidad,
# MAGIC métricas, sinónimos y benchmarks (no solo para explorar).