# Databricks notebook source
# MAGIC %md
# MAGIC # 04b — 🛡️ Política de Uso de Datos: No-Entrenamiento con Datos del Cliente
# MAGIC
# MAGIC **15 min.** Documenta y **verifica** la postura de Databricks sobre el uso de datos: **tus datos y prompts NO se usan para entrenar modelos** (ni de Databricks ni de los proveedores de modelos), y cómo configurar/auditar esa garantía.
# MAGIC
# MAGIC > Módulo **aditivo** — complementa el `04`. Cierra el gap **opt-out / no-training-on-customer-data (concern 1c)** de la scorecard de gobernanza de proveedores de IA.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este módulo — **Secuencial (Prosa/UI → Code)**
# MAGIC
# MAGIC Una postura de uso de datos es primero **política contractual y de configuración** (prosa + settings en la UI: AI Gateway, model serving, abuse logging). Pero una afirmación de gobernanza no basta como texto: la **verificamos por código** (system tables, config de endpoints, inference tables) para que sea **auditable**. Documentamos la postura y los toggles en la UI, y luego **evidenciamos** por código qué se loggea, dónde y bajo control de quién.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1 — La postura (lo que hay que poder afirmar)
# MAGIC
# MAGIC | Afirmación | Detalle |
# MAGIC |---|---|
# MAGIC | **No-entrenamiento** | Databricks **no usa** los datos del cliente (tablas, prompts, respuestas, archivos) para entrenar sus modelos base ni los comparte con terceros para ese fin. |
# MAGIC | **Aislamiento del tenant** | Datos, modelos y endpoints viven dentro de **tu** workspace/cuenta y se gobiernan con Unity Catalog (permisos, lineage, auditoría). |
# MAGIC | **Foundation Model APIs** | Las llamadas a los modelos servidos por Databricks (Llama, Claude vía Databricks, etc.) **no** alimentan entrenamiento. Los proveedores externos accedidos vía Databricks operan bajo acuerdos *zero-retention / no-training*. |
# MAGIC | **Opt-out de logging** | Cualquier *payload logging* (Inference Tables, AI Gateway) es **opcional**, lo activas tú, queda en **tu** Unity Catalog y lo controlas/borras tú. |
# MAGIC
# MAGIC > 📄 La fuente contractual es el **DPA (Data Processing Addendum)** y la documentación de seguridad de Databricks. Este módulo te da cómo **verificarlo técnicamente** dentro del producto.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2 — Configurar / verificar en la UI (🖱️)
# MAGIC
# MAGIC 1. **Model Serving / Foundation Models:** Sidebar → **Serving** → tu endpoint → **Events / Settings**. El **payload logging (Inference Tables)** es un toggle **opcional**. Si lo activas, los requests se guardan en **una tabla Delta de tu UC** — no salen de tu tenant.
# MAGIC 2. **AI Gateway:** en endpoints con Gateway, **Usage tracking** e **Inference tables** son configurables; los guardrails y el logging escriben a **tus** system/Delta tables.
# MAGIC 3. **Account Console → Settings → Security/Compliance:** revisa la postura de la cuenta (perfiles de compliance, retención). Las opciones de *abuse monitoring* de proveedores externos son configurables a nivel de cuenta.
# MAGIC 4. **Decisión de gobernanza:** si una carga es ultra-sensible, **desactiva** el payload logging para ese endpoint (no se persiste ningún prompt/respuesta), o restringe la tabla de inference con permisos UC.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 3 — Verificación por código (auditable)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# ¿Qué endpoints tienen payload/inference logging activado? (transparencia: qué se persiste)
print("Auditoría de logging por endpoint de serving:\n")
try:
    for e in w.serving_endpoints.list():
        gw = getattr(e, "ai_gateway", None)
        inf = getattr(gw, "inference_table_config", None) if gw else None
        estado = "ON" if inf else "off / no configurado"
        print(f"  {e.name:<45}  inference logging: {estado}")
except Exception as ex:
    print("  (requiere permisos sobre serving endpoints):", ex)

print("""
Lectura: 'off' significa que ese endpoint NO persiste prompts/respuestas.
Si está 'ON', los datos van a una tabla Delta de TU Unity Catalog, bajo TUS permisos.
""")

# COMMAND ----------

# Confirmar que el modelo y sus datos viven en TU catálogo (aislamiento del tenant)
print(f"Modelo registrado en (tu UC, tu tenant): {MODEL_NAME}")
print(f"Datos de entrenamiento en:               {CATALOG}.{SCHEMA}.mlops_churn_training")
print("\nNada de esto se replica fuera de tu workspace/cuenta para entrenamiento de terceros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 4 — Patrón recomendado para datos sensibles
# MAGIC
# MAGIC 1. **Desactiva** inference tables en endpoints que procesen datos ultra-sensibles (o enmascara con AI Gateway guardrails / `ai_mask`).
# MAGIC 2. Si necesitas logging para auditoría, **mantenlo en tu UC** con permisos estrictos y política de retención (borrado programado).
# MAGIC 3. Documenta en tu *runbook* de gobernanza: qué endpoint loggea, a qué tabla, quién accede, cuánto se retiene — todo verificable con las celdas de arriba.
# MAGIC
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Postura documentada: **no-entrenamiento** con datos del cliente + aislamiento de tenant.
# MAGIC ✅ Configurable/auditable: payload logging **opcional**, dentro de tu UC, bajo tu control.
# MAGIC ✅ Verificación por código del estado de logging por endpoint y de dónde viven datos/modelo.
# MAGIC ✅ Patrón **Prosa/UI → Code**: política + settings en la UI, evidencia por código.
# MAGIC
# MAGIC ## Continuar → `05 - Model Serving (UI + API)`
