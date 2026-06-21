# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — LAB 🛡️ · Gobernanza con Unity Catalog (asistida por IA)
# MAGIC
# MAGIC **30 min.** Documentas, clasificas y proteges datos sensibles. Primero **a mano en la UI** (para entender el control), luego **automatizado con IA** sobre todo el esquema.
# MAGIC
# MAGIC Adaptado de `Data_governace` (clasificación + masking con IA, contexto banco digital / LGPD).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este módulo — **Secuencial (UI → Code)**
# MAGIC
# MAGIC La gobernanza tiene dos planos. **Primero la UI**: en Catalog Explorer aplicas un *tag* y un *column mask* a una columna **con clicks**, para *ver y entender* qué hace el control y cómo cambia lo que ven distintos usuarios. **Luego el código**: usas IA (`ai_gen`, `ai_query`) para **documentar y clasificar todo el esquema** y aplicar tags/masks en bucle — lo que a mano sería inviable. La UI **enseña**, el código **escala**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — crear un esquema de banco digital con datos sensibles

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")
print(f"{CATALOG}.{SCHEMA}")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gov_clientes AS
SELECT * FROM VALUES
  (1001, 'Maria Fernanda Lima', '12345678901', DATE'1988-03-14', 'maria.lima@email.com', '3119876543', 'Bogotá', 'activo'),
  (1002, 'Carlos Andrés Ruiz',  '98765432100', DATE'1990-07-22', 'carlos.ruiz@email.com', '3157654321', 'Medellín', 'activo'),
  (1003, 'Ana Sofía Torres',    '45678912300', DATE'1985-11-05', 'ana.torres@email.com',  '3201234567', 'Cali', 'inactivo'),
  (1004, 'Luis Mejía',          '32165498700', DATE'1995-01-30', 'luis.mejia@email.com',   '3009876543', 'Bogotá', 'activo')
AS t(cliente_id, nombre_completo, documento_id, fecha_nacimiento, email, telefono, ciudad, estado_cliente)
""")
display(spark.table("gov_clientes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte A — Gobernanza a MANO en la UI (🖱️) — entender el control
# MAGIC
# MAGIC 1. **Catalog → `ardemo_classic_dnubtw_catalog` → `ws_<usuario>` → `gov_clientes`**.
# MAGIC 2. Tab **Columns** → columna `documento_id` → menú **⋮ → Set tags** → agrega `clasificacion = SENSIBLE`. (Así se etiqueta a mano.)
# MAGIC 3. Tab **Permissions** → **Grant** → observa cómo darías `SELECT` a un grupo con clicks.
# MAGIC 4. (Opcional) **Lineage**: si esta tabla viniera de un pipeline, verías su origen.
# MAGIC
# MAGIC > Acabas de aplicar **un** tag a **una** columna. Ahora imagina 10 tablas × 27 columnas. Eso es lo que automatizamos con código.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte B — Código: comentarios de columnas con `ai_gen`
# MAGIC
# MAGIC Documentar acelera el descubrimiento y el razonamiento de Genie/agentes. Generamos comentarios con IA para cada columna.

# COMMAND ----------

def comentario_ia(table, col, dtype):
    prompt = (f"Describe de forma objetiva y breve (máx 200 caracteres) la finalidad de la columna "
              f"'{col}' (tipo {dtype}) de la tabla '{table}' en el contexto de un banco digital. "
              f"No repitas el nombre de la columna ni el tipo. Empieza el texto con '[IA] '. "
              f"Devuelve solo el comentario, sin comillas.")
    return spark.sql(f"SELECT ai_gen('{prompt}') AS c").collect()[0]["c"]

cols = spark.sql(f"""
  SELECT table_name, column_name, data_type
  FROM system.information_schema.columns
  WHERE table_catalog = '{CATALOG}' AND table_schema = '{SCHEMA}' AND table_name = 'gov_clientes'
""").collect()

for r in cols:
    c = comentario_ia(r.table_name, r.column_name, r.data_type).replace("'", "")
    spark.sql(f"ALTER TABLE {r.table_name} ALTER COLUMN {r.column_name} COMMENT '{c}'")
    print(f"  {r.column_name}: {c}")
print("\n✅ Comentarios generados. Refresca la tabla en Catalog Explorer para verlos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte C — Código: clasificar columnas con `ai_query` (salida JSON estructurada)
# MAGIC
# MAGIC Clasificamos cada columna en **SENSIBLE / CONFIDENCIAL / PUBLICO** usando `responseFormat` con JSON schema (salida confiable).

# COMMAND ----------

prompt_clf = ("Eres un agente de gobernanza de datos. Clasifica la columna de un banco digital en una de: "
              "SENSIBLE (datos personales protegidos: documento, email, teléfono, fecha nacimiento), "
              "CONFIDENCIAL (datos internos restringidos: saldos, estados), "
              "PUBLICO (sin restricción: ciudad, ids técnicos). ")

clasificadas = spark.sql(f"""
SELECT table_name, column_name, data_type,
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    concat('{prompt_clf}',
           'Devuelve JSON con classification y reasoning. Columna: ', column_name,
           '. Tabla: ', table_name, '. Tipo: ', data_type),
    responseFormat => '{{
      "type": "json_schema",
      "json_schema": {{"name": "clasif", "schema": {{"type": "object", "properties": {{
        "classification": {{"type": "string"}}, "reasoning": {{"type": "string"}}
      }}}}, "strict": true}}
    }}'
  ) AS json_clf
FROM system.information_schema.columns
WHERE table_catalog = '{CATALOG}' AND table_schema = '{SCHEMA}' AND table_name = 'gov_clientes'
""")

from pyspark.sql import functions as F
clasificadas = (clasificadas
    .withColumn("classification", F.get_json_object("json_clf", "$.classification"))
    .withColumn("reasoning", F.get_json_object("json_clf", "$.reasoning")))
clasificadas.cache()
display(clasificadas.select("column_name", "classification", "reasoning"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte D — Código: aplicar tags en bucle
# MAGIC
# MAGIC Lo que en la UI hiciste con una columna, ahora a todas — automáticamente.

# COMMAND ----------

for r in clasificadas.collect():
    cls = (r.classification or "PUBLICO").strip().upper()
    spark.sql(f"ALTER TABLE {r.table_name} ALTER COLUMN {r.column_name} SET TAGS ('clasificacion' = '{cls}')")
    print(f"  {r.column_name} → {cls}")
print("\n✅ Tags aplicados. Compruébalos en Catalog Explorer (los verás junto al que pusiste a mano).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte E — Código: column masking sobre las columnas SENSIBLE
# MAGIC
# MAGIC Creamos una función de máscara y la aplicamos a las columnas clasificadas como SENSIBLE. Quien no pertenezca al grupo verá el valor enmascarado.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.mask_sensible(v STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('admins') THEN v ELSE '***SENSIBLE***' END
""")

for r in clasificadas.filter(F.col("classification") == "SENSIBLE").collect():
    # solo aplicamos máscara a columnas STRING
    if r.data_type.lower() in ("string", "varchar"):
        spark.sql(f"""ALTER TABLE {r.table_name}
                      ALTER COLUMN {r.column_name}
                      SET MASK {CATALOG}.{SCHEMA}.mask_sensible""")
        print(f"  MASK aplicado a {r.column_name}")

print("\nResultado (si no eres admin, las columnas sensibles aparecen enmascaradas):")
display(spark.table("gov_clientes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte F — Verificar en la UI (🖱️)
# MAGIC
# MAGIC Vuelve a `gov_clientes` en Catalog Explorer:
# MAGIC - **Columns**: comentarios `[IA] ...` + tags `clasificacion` en todas las columnas.
# MAGIC - La columna `documento_id` ahora tiene **mask** (icono).
# MAGIC - **Filtra el catálogo por tag** `clasificacion = SENSIBLE` para encontrar datos sensibles en todo el workspace.
# MAGIC
# MAGIC > 💡 Cerraste el ciclo **UI → Code → UI**: entendiste el control con clicks, lo escalaste con IA, y lo verificaste/gobiernas de nuevo en la UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Aplicaste un tag/mask **a mano** (UI) para entender el control
# MAGIC ✅ Generaste comentarios con `ai_gen`
# MAGIC ✅ Clasificaste columnas con `ai_query` + JSON schema
# MAGIC ✅ Aplicaste **tags** y **column masks** en bucle (código)
# MAGIC ✅ Verificaste y gobernaste el resultado en Catalog Explorer
# MAGIC
# MAGIC ## Continuar → `03 - LAB AI Functions (SQL)`
