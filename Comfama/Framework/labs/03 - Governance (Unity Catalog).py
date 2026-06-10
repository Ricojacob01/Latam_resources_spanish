# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Governance con Unity Catalog
# MAGIC
# MAGIC **Reemplaza**: la capa `security/` del repo (`AuthManager`, `SecretProvider`, `AzureKeyVaultProvider`, + auditoría manual)
# MAGIC
# MAGIC ## ¿Qué hace su código actual?
# MAGIC
# MAGIC En `comfama-ai-core/security/` mantienen:
# MAGIC - `AuthManager` para FastAPI/MCP/A2A (JWT RS256 + Entra ID + JWKS cache)
# MAGIC - `SecretProvider` con factory y backend Azure Key Vault
# MAGIC - Audit logs custom escritos a Log Analytics
# MAGIC - Permisos manuales por bucket / DB / servicio
# MAGIC
# MAGIC ## ¿Qué hace Databricks?
# MAGIC
# MAGIC **Unity Catalog** entrega gobernanza nativa de:
# MAGIC - **Permisos**: GRANT/REVOKE sobre tablas, vectores, modelos, funciones, volumes
# MAGIC - **ABAC (Attribute-Based Access Control)**: políticas por atributo (PII, sensibilidad, región)
# MAGIC - **Linaje automático**: cada query se rastrea automáticamente, sin instrumentación
# MAGIC - **Audit logs nativos**: cada acceso queda en `system.access.audit`
# MAGIC - **Row/Column-level security**: funciones de filtro reutilizables
# MAGIC - **Tags y comments**: governance documentado en metadatos
# MAGIC
# MAGIC **Tiempo estimado:** 7 min

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
SCHEMA = "comfama"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Permisos y propiedad

# COMMAND ----------

# Ver el owner del schema
display(spark.sql(f"DESCRIBE SCHEMA EXTENDED {FULL_SCHEMA}"))

# COMMAND ----------

# Ver grants vigentes sobre el schema
display(spark.sql(f"SHOW GRANTS ON SCHEMA {FULL_SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### GRANT pattern — equivalente a su lógica de `AuthManager` pero declarativo
# MAGIC
# MAGIC En vez de un middleware FastAPI con JWT que decide qué endpoints puede llamar el usuario, Unity Catalog evalúa permisos a nivel de objeto en cada query:

# COMMAND ----------

# Ejemplo: dar permisos selectivos a un grupo (no se ejecuta — sólo de muestra)
GRANT_EXAMPLES = """
-- Equivalente a su lógica de roles en AuthManager:

-- Analistas pueden leer Gold pero NO los datos crudos del agente
GRANT USE_CATALOG ON CATALOG ardemo_classic_dnubtw_catalog TO `comfama-analistas`;
GRANT USE_SCHEMA ON SCHEMA ardemo_classic_dnubtw_catalog.comfama TO `comfama-analistas`;
GRANT SELECT ON TABLE ardemo_classic_dnubtw_catalog.comfama.metricas_agente_gold TO `comfama-analistas`;

-- Equipo de plataforma puede leer Bronze/Silver para troubleshooting
GRANT SELECT ON TABLE ardemo_classic_dnubtw_catalog.comfama.eventos_agente_bronze TO `comfama-plataforma`;
GRANT SELECT ON TABLE ardemo_classic_dnubtw_catalog.comfama.eventos_agente_silver TO `comfama-plataforma`;

-- DPO (Data Protection Officer) puede ver TODO incluyendo PII
GRANT ALL PRIVILEGES ON SCHEMA ardemo_classic_dnubtw_catalog.comfama TO `comfama-dpo`;
"""
print(GRANT_EXAMPLES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tags para governance (PII, sensibilidad)
# MAGIC
# MAGIC Marcamos las columnas que contienen información sensible. Esto reemplaza los "data classifications" manuales del repo.

# COMMAND ----------

# Nota: este workspace tiene una "tag policy" que restringe valores de algunos tags
# (ej. pii sólo acepta ssn/address). Usamos tag keys propios del demo:
try:
    spark.sql(f"""
    ALTER TABLE {FULL_SCHEMA}.eventos_agente_silver
    ALTER COLUMN user_id SET TAGS ('data_classification' = 'pii_indirect', 'sensibilidad' = 'media')
    """)
    spark.sql(f"""
    ALTER TABLE {FULL_SCHEMA}.eventos_agente_silver
    ALTER COLUMN intent SET TAGS ('dominio' = 'agente_ia', 'sensibilidad' = 'baja')
    """)
    spark.sql(f"""
    ALTER TABLE {FULL_SCHEMA}.metricas_agente_gold
    SET TAGS ('layer' = 'gold', 'dominio' = 'agente_ia', 'criticidad' = 'alta')
    """)
    print("✓ Tags aplicados")
except Exception as e:
    print(f"⚠ Tag policy restringe valores: {e}")
    print("  En un metastore sin policy esto funciona directo. Aquí ajustamos.")

# COMMAND ----------

# Ver tags de columnas — útil para audit + discovery
display(spark.sql(f"""
SELECT table_name, column_name, tag_name, tag_value
FROM system.information_schema.column_tags
WHERE catalog_name = '{CATALOG}' AND schema_name = '{SCHEMA}'
ORDER BY table_name, column_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Column masking para PII (reemplaza enmascarado manual)
# MAGIC
# MAGIC Creamos una función de mascarado y la aplicamos a `user_id`. Cualquier query que no sea del owner verá los valores enmascarados.

# COMMAND ----------

# Función de mascarado: solo grupos privilegiados ven el valor real
spark.sql(f"""
CREATE OR REPLACE FUNCTION {FULL_SCHEMA}.mask_user_id(user_id STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('comfama-dpo') THEN user_id
    WHEN is_account_group_member('comfama-plataforma') THEN concat(substring(user_id, 1, 5), '***')
    ELSE 'REDACTED'
  END
""")

print("✓ Función de mascarado creada")
print("  - DPO ve el user_id completo")
print("  - Plataforma ve los primeros 5 chars + '***'")
print("  - Resto del mundo ve 'REDACTED'")

# COMMAND ----------

# Aplicar el masking a la columna (comentado — requiere los grupos creados):
masking_example = f"""
ALTER TABLE {FULL_SCHEMA}.eventos_agente_silver
ALTER COLUMN user_id SET MASK {FULL_SCHEMA}.mask_user_id;
"""
print("Para activarlo:")
print(masking_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Lineage automático
# MAGIC
# MAGIC En su framework actual, para saber "qué consume los datos de `ai_execution_runs`" tienen que buscar manualmente en el código. UC lo rastrea automáticamente para todo: cada SELECT, JOIN, INSERT, MERGE deja huella.

# COMMAND ----------

# Linaje desde la perspectiva de la tabla Gold
display(spark.sql(f"""
SELECT
  source_table_full_name,
  source_table_catalog,
  source_table_schema,
  source_table_name,
  source_type,
  event_time
FROM system.access.table_lineage
WHERE target_table_full_name = '{FULL_SCHEMA}.metricas_agente_gold'
  AND event_date >= current_date() - INTERVAL 7 DAYS
ORDER BY event_time DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Audit logs nativos
# MAGIC
# MAGIC Cada acceso a las tablas queda registrado en `system.access.audit`. Esto reemplaza el audit log custom que escriben a Log Analytics.

# COMMAND ----------

# Accesos recientes al schema comfama
display(spark.sql(f"""
SELECT
  event_time,
  user_identity.email as user_email,
  service_name,
  action_name,
  request_params.statement as statement_preview,
  response.status_code as status
FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 1 DAYS
  AND (
    request_params.full_name_arg LIKE '{CATALOG}.{SCHEMA}.%'
    OR request_params.statement LIKE '%{SCHEMA}%'
  )
ORDER BY event_time DESC
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Documentación con COMMENT (governance auto-documentado)
# MAGIC
# MAGIC Su framework actual depende de README + docstrings. UC permite documentar directamente en metadatos, queryable.

# COMMAND ----------

spark.sql(f"""
COMMENT ON TABLE {FULL_SCHEMA}.metricas_agente_gold IS
'Métricas agregadas por hora del agente conversacional Comfama. Replaces ai_execution_runs aggregations.
Owner: Plataforma IA. Refresh: hourly. Criticality: HIGH (alimenta SQL Alerts + dashboards).'
""")

spark.sql(f"""
COMMENT ON COLUMN {FULL_SCHEMA}.metricas_agente_gold.feedback_score_avg IS
'Promedio de feedback de usuarios. Range: [-1, 1]. -1=thumbs_down, 0=neutral/none, 1=thumbs_up.'
""")

print("✓ Documentación aplicada")
display(spark.sql(f"DESCRIBE TABLE EXTENDED {FULL_SCHEMA}.metricas_agente_gold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Comparación side-by-side
# MAGIC
# MAGIC | Capacidad | Su código actual | Unity Catalog |
# MAGIC |---|---|---|
# MAGIC | Auth | `AuthManager` (JWT + Entra + JWKS) | Databricks SSO + Service Principals |
# MAGIC | Secretos | `SecretProvider` factory | Secret Scopes (con Azure KV backed) |
# MAGIC | Permisos por tabla | Lógica en FastAPI + middleware | `GRANT` SQL declarativo |
# MAGIC | Permisos por columna | Filtros en aplicación | Column mask functions |
# MAGIC | PII tagging | Comentarios en código + classifications manuales | Column tags + filter policies |
# MAGIC | Audit | Inserts custom a Log Analytics | `system.access.audit` (nativo) |
# MAGIC | Linaje | Documentación manual | `system.access.table_lineage` (automático) |
# MAGIC | Discovery | README + Wiki | UC Search + tags queryables |
# MAGIC | **LOC en su repo** | **~400** | **~0 (SQL declarativo)** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximo paso
# MAGIC
# MAGIC Continuar con: `04 - Monitoring (Lakehouse Monitoring)` — usaremos las tablas que acabamos de gobernar para configurar monitores de calidad automáticos.

