# Databricks notebook source
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # 07b — 🔎 Auditoría y Trazabilidad con System Tables
# MAGIC
# MAGIC **25 min.** Lab hands-on para responder *quién hizo qué, cuándo y sobre qué* usando las **system tables** de Databricks: `system.access.audit_logs`, lineage y eventos de despliegue de modelos. Observabilidad y traza para gobernanza.
# MAGIC
# MAGIC > Módulo **aditivo** — complementa la orquestación del `07`. Consultas de auditoría para gobernanza de modelos y datos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este módulo — **Lado a lado (Catalog UI ↔ SQL)**
# MAGIC
# MAGIC Las system tables son **datos gobernados** como cualquier otro: se **exploran en Catalog Explorer** (UI) — navegas el catálogo `system`, ves esquemas (`access`, `billing`, `lakeflow`, `serving`…) y el schema de cada tabla — y se **consultan en SQL** (código). Lo presentamos **lado a lado** porque son la misma capacidad: la UI para *descubrir* qué hay disponible, el SQL para *responder* la pregunta de auditoría de forma reproducible (y agendable como query/alerta).

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 0 — Habilitar / descubrir system tables (🖱️ UI + código)
# MAGIC
# MAGIC **UI:** Catalog Explorer → catálogo **`system`**. Verás esquemas como `access`, `billing`, `compute`, `lakeflow`, `serving`, `query`. Abre `system.access.audit_logs` → tab **Columns** para ver su esquema.
# MAGIC
# MAGIC El esquema `system.access` (audit_logs, table_lineage, column_lineage) puede requerir habilitación una sola vez por un **account admin** (vía API/CLI). Esta celda comprueba si está accesible.

# COMMAND ----------

def schema_accesible(full):
    try:
        spark.sql(f"SELECT 1 FROM {full} LIMIT 1")
        return True
    except Exception as e:
        print(f"  ⚠ {full} no accesible: {str(e)[:120]}")
        return False

print("Disponibilidad de system tables clave:")
for t in ["system.access.audit_logs", "system.access.table_lineage",
          "system.access.column_lineage"]:
    print(f"  {'✅' if schema_accesible(t) else '❌'}  {t}")

print("""
Si alguna sale ❌, un account admin la habilita una vez:
  databricks api patch /api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas/access
  (o: Account Console → habilitar system schemas). Las queries de abajo funcionan al habilitarse.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — ¿Quién hizo qué? (eventos recientes) — concern 3a/6a

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Actividad reciente: usuario, acción, servicio, recurso
# MAGIC SELECT
# MAGIC   event_time,
# MAGIC   user_identity.email          AS usuario,
# MAGIC   service_name,
# MAGIC   action_name,
# MAGIC   request_params,
# MAGIC   source_ip_address
# MAGIC FROM system.access.audit_logs
# MAGIC WHERE event_date >= current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Despliegues y cambios de modelos (MLOps traceability)
# MAGIC
# MAGIC Quién registró, asignó alias o desplegó modelos — la traza del ciclo MLOps de los módulos 04/05/07.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Eventos de Model Registry (UC) y Model Serving
# MAGIC SELECT
# MAGIC   event_time,
# MAGIC   user_identity.email AS usuario,
# MAGIC   service_name,
# MAGIC   action_name,
# MAGIC   request_params
# MAGIC FROM system.access.audit_logs
# MAGIC WHERE event_date >= current_date() - INTERVAL 30 DAYS
# MAGIC   AND (
# MAGIC        action_name ILIKE '%registeredModel%'      -- crear/actualizar modelo en UC
# MAGIC     OR action_name ILIKE '%modelVersion%'         -- nuevas versiones / alias
# MAGIC     OR action_name ILIKE '%servingEndpoint%'      -- crear/actualizar endpoint
# MAGIC     OR service_name = 'serverlessRealTimeInference'
# MAGIC   )
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Eventos de acceso a datos (quién leyó/escribió qué tabla)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Accesos a Unity Catalog: lecturas/escrituras sobre tablas sensibles
# MAGIC SELECT
# MAGIC   event_time,
# MAGIC   user_identity.email AS usuario,
# MAGIC   action_name,
# MAGIC   request_params.full_name_arg AS objeto,
# MAGIC   request_params.operation     AS operacion
# MAGIC FROM system.access.audit_logs
# MAGIC WHERE service_name = 'unityCatalog'
# MAGIC   AND event_date >= current_date() - INTERVAL 7 DAYS
# MAGIC   AND action_name ILIKE '%getTable%' OR action_name ILIKE '%generateTemporaryTableCredential%'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Linaje (de dónde viene un dato/modelo) — observabilidad

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lineage de tablas: upstream/downstream del dataset de entrenamiento de churn
# MAGIC SELECT
# MAGIC   source_table_full_name,
# MAGIC   target_table_full_name,
# MAGIC   entity_type,
# MAGIC   event_time
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE (source_table_full_name ILIKE '%mlops_churn%'
# MAGIC     OR target_table_full_name ILIKE '%mlops_churn%')
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Convertir una query de auditoría en control continuo (🖱️ + código)
# MAGIC
# MAGIC Una query de auditoría sirve poco si se corre a mano una vez. Dos formas de operacionalizarla:
# MAGIC
# MAGIC - **UI:** guarda la query del Paso 2 en **SQL Editor**, créale una **Alert** (p.ej. "avísame si alguien crea/actualiza un serving endpoint fuera del Job de CI/CD") y/o un **dashboard AI/BI** de auditoría.
# MAGIC - **Código:** agrégala como una **tarea de auditoría** al Job del módulo 07 (o un Job aparte) para materializar diariamente una tabla `gold_auditoria_mlops` y monitorearla.

# COMMAND ----------

# Ejemplo: materializar un resumen de auditoría de MLOps (idempotente).
try:
    spark.sql(f"""
      CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_auditoria_mlops AS
      SELECT date(event_time) AS dia,
             user_identity.email AS usuario,
             action_name,
             count(*) AS eventos
      FROM system.access.audit_logs
      WHERE event_date >= current_date() - INTERVAL 30 DAYS
        AND (action_name ILIKE '%registeredModel%'
          OR action_name ILIKE '%modelVersion%'
          OR action_name ILIKE '%servingEndpoint%')
      GROUP BY 1, 2, 3
      ORDER BY dia DESC
    """)
    print(f"✓ Tabla de auditoría creada: {CATALOG}.{SCHEMA}.gold_auditoria_mlops")
    display(spark.table(f"{CATALOG}.{SCHEMA}.gold_auditoria_mlops"))
except Exception as e:
    print("Requiere system.access.audit_logs habilitado (ver Paso 0). Detalle:", str(e)[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Consultaste `system.access.audit_logs`: **quién hizo qué** (3a/6a).
# MAGIC ✅ Trazaste **despliegues/cambios de modelos** y **accesos a datos**.
# MAGIC ✅ Usaste **lineage** (`system.access.table_lineage`) para observabilidad.
# MAGIC ✅ Operacionalizaste la auditoría: Alert/dashboard en la UI o tarea de Job en código.
# MAGIC ✅ Patrón **Lado a lado (Catalog UI ↔ SQL)**: descubrir en la UI, responder en SQL.
# MAGIC
# MAGIC ## Continuar → `08 - Cierre y Recap`
