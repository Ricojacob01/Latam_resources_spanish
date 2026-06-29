# Databricks notebook source
# MAGIC %md
# MAGIC # 🔐 Sesión 2 · 03 — Gobernanza (Unity Catalog)
# MAGIC
# MAGIC **Meta:** gobernar los datos del afiliado con **Unity Catalog**: ver **lineage** de punta a punta, aplicar
# MAGIC **seguridad a nivel de fila/columna (ABAC)** sobre datos sensibles, y auditar **quién hizo qué**.
# MAGIC
# MAGIC > **Equivale a: `security/`** del framework Comfama. En vez de código de permisos custom, UC da gobierno
# MAGIC > declarativo, lineage automático y auditoría centralizada.
# MAGIC
# MAGIC Módulo **dual-mode**: lineage/permite en **🖱️ Catalog Explorer**; máscaras y filtros en **⌨️ SQL**.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🖱️ Camino UI — Lineage y permisos
# MAGIC 1. **Catalog** → `ardemo_classic_dnubtw_catalog` → tu schema → tabla **`kb_documentos`** → pestaña **Lineage**.
# MAGIC    Verás el flujo: `kb_documentos → índice Vector Search → agente`.
# MAGIC 2. Tabla **`afiliados`** → pestaña **Permissions**: aquí se otorgan/revocan accesos (GRANT) por grupo.
# MAGIC 3. Pestaña **Insights/History** del modelo y tablas para ver uso.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⌨️ Camino SQL — ABAC: enmascarar PII y filtrar por ciudad
# MAGIC Los datos del afiliado tienen PII (`documento`, `email`). Aplicamos **column masks** y un **row filter** para que
# MAGIC solo usuarios autorizados vean datos completos.

# COMMAND ----------

# Column mask: enmascara el documento salvo para miembros del grupo 'comfama_admins'
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.mask_documento(doc STRING)
RETURN CASE WHEN is_account_group_member('comfama_admins') THEN doc
            ELSE CONCAT('****', RIGHT(doc, 3)) END
""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.mask_email(em STRING)
RETURN CASE WHEN is_account_group_member('comfama_admins') THEN em
            ELSE regexp_replace(em, '(^.).*(@.*$)', '$1***$2') END
""")

# Aplicar las máscaras a las columnas
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.afiliados ALTER COLUMN documento SET MASK {CATALOG}.{SCHEMA}.mask_documento")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.afiliados ALTER COLUMN email SET MASK {CATALOG}.{SCHEMA}.mask_email")
print("✅ Column masks aplicadas a afiliados.documento y afiliados.email")

# COMMAND ----------

# Row filter (demo): admins ven todo; el resto solo 'Medellín'
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.filtro_ciudad(ciudad STRING)
RETURN is_account_group_member('comfama_admins') OR ciudad = 'Medellín'
""")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.afiliados SET ROW FILTER {CATALOG}.{SCHEMA}.filtro_ciudad ON (ciudad)")
print("✅ Row filter aplicado sobre afiliados (por ciudad)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificar el efecto
# MAGIC Como no eres miembro de `comfama_admins`, deberías ver el documento enmascarado y solo filas de 'Medellín'.

# COMMAND ----------

display(spark.sql(f"SELECT afiliado_id, nombre, documento, email, ciudad FROM {CATALOG}.{SCHEMA}.afiliados"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Auditoría — quién consultó / reservó
# MAGIC Las acciones quedan en `system.access.audit`. Útil para responder "¿quién accedió a datos de afiliados?".

# COMMAND ----------

display(spark.sql(f"""
  SELECT event_time, user_identity.email AS usuario, action_name, request_params
  FROM system.access.audit
  WHERE event_date >= current_date() - INTERVAL 7 DAYS
    AND (request_params.full_name_arg LIKE '%{SCHEMA}%' OR action_name ILIKE '%table%')
  ORDER BY event_time DESC
  LIMIT 25
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 (Opcional) Quitar máscaras/filtro para no afectar otros módulos
# MAGIC ```sql
# MAGIC -- ALTER TABLE ... ALTER COLUMN documento DROP MASK;
# MAGIC -- ALTER TABLE ... DROP ROW FILTER;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resultado
# MAGIC Gobierno declarativo: lineage automático, PII enmascarada, acceso por fila y auditoría central — sin código de
# MAGIC seguridad custom.
# MAGIC
# MAGIC ### ▶️ Siguiente: `04 - Monitoreo + Alertas`

