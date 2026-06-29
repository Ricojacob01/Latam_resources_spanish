# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png">
# MAGIC
# MAGIC # 🤖 Comfama — Workshop Agentes en Producción
# MAGIC ## Sesión 1 · 00 — Bienvenida y Agenda
# MAGIC
# MAGIC ¡Bienvenidos! En estas **2 sesiones de 3 horas** vamos a construir, de punta a punta, un **agente de IA listo
# MAGIC para producción** sobre Databricks — y, en el camino, veremos el **equivalente managed de Databricks** para cada
# MAGIC pieza del framework de IA que Comfama mantiene hoy en Azure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Caso de uso: *Agente de Servicios al Afiliado Comfama*
# MAGIC
# MAGIC Un asistente de autoservicio que **no solo responde — transacciona**. El afiliado puede:
# MAGIC - **Preguntar** por beneficios y programas (recreación, educación, salud, subsidios) → **RAG**.
# MAGIC - **Consultar** sus inscripciones y la disponibilidad de un programa → lectura **OLTP** en Lakebase.
# MAGIC - **Reservar** un cupo → **escritura transaccional** en Lakebase (verifica cupo → inserta → descuenta, atómico).
# MAGIC
# MAGIC Esa última acción es la que pone a **Lakebase en el centro**: un demo solo-analítico no necesitaría OLTP; un
# MAGIC agente que reserva cupos, sí.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗺️ Agenda de las 2 sesiones
# MAGIC
# MAGIC ### Sesión 1 — Construir y Servir el Agente (hoy)
# MAGIC | # | Módulo | Equivale a |
# MAGIC |---|---|---|
# MAGIC | 00 | Bienvenida y Agenda | — |
# MAGIC | 01 | Product Tour (Agente end-to-end) | (mapa general) |
# MAGIC | 02 | Setup & Knowledge Base | — |
# MAGIC | 03 | Lakebase (datos del afiliado) | **Cosmos DB** |
# MAGIC | 04 | Construir el Agente | `TemplateAgentes` |
# MAGIC | 05 | Servir el Agente | — |
# MAGIC | 06 | AI Gateway | `LLMConfig + TokenProvider` |
# MAGIC | 07 | Cierre Sesión 1 | — |
# MAGIC
# MAGIC ### Sesión 2 — Producción y Deploy (siguiente)
# MAGIC App · Observabilidad · Gobernanza · Monitoreo+Alertas · FinOps · **Deploy-as-Code para su framework** · Cierre

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🖱️ Cómo usar este workshop: **UI primero, código como alternativa**
# MAGIC
# MAGIC Cada módulo está pensado para dos caminos — **elige el tuyo**:
# MAGIC
# MAGIC 1. **UI primero** 🖱️ — sigue las **instrucciones paso a paso** (qué pantalla, qué botón). Puedes completar el
# MAGIC    módulo **sin escribir código**.
# MAGIC 2. **Celda ejecutable** ⌨️ — junto a los pasos hay una **celda que hace lo mismo**. Si prefieres, **ejecútala**.
# MAGIC
# MAGIC > Ambos caminos dejan el mismo asset desplegado. El **código de automatización** (Asset Bundle · API · SDK) se
# MAGIC > concentra al final (Sesión 2), como ejemplo de integración con el **framework de agentes de Comfama**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Pre-check
# MAGIC Ejecuta la siguiente celda para verificar que tienes lo necesario. Si algo falla, avísanos antes de continuar.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

print("🔎 Verificando entorno...\n")

# 1) Usuario y compute
print(f"Usuario:          {current_user}")
print(f"Catálogo/Schema:  {CATALOG}.{SCHEMA}")

# 2) Acceso a Foundation Models (LLM que usará el agente)
try:
    from mlflow.deployments import get_deploy_client
    client = get_deploy_client("databricks")
    resp = client.predict(endpoint=LLM_ENDPOINT,
                          inputs={"messages":[{"role":"user","content":"Responde solo 'ok'"}],
                                  "max_tokens": 5})
    print(f"Foundation Model: ✅ {LLM_ENDPOINT} responde")
except Exception as e:
    print(f"Foundation Model: ⚠️  no pude invocar {LLM_ENDPOINT} ({type(e).__name__}). "
          f"Revisa permisos de Model Serving / FM APIs.")

# 3) Tablas semilla listas
tablas = [r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()]
for t in ["programas","afiliados","beneficios_afiliado","kb_documentos"]:
    print(f"Tabla {t:22} {'✅' if t in tablas else '❌ falta'}")

print("\n¡Listo! Si todo está en ✅ puedes pasar al módulo 01.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ▶️ Siguiente: `01 - Product Tour (Agente end-to-end)`

