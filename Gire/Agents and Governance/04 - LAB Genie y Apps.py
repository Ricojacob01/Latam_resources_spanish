# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — LAB 💬 · Genie y Apps
# MAGIC
# MAGIC **35 min.** Creas un **Genie space** (lenguaje natural sobre tus datos) y una **App Streamlit** que lo consume vía el SDK.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este módulo — **Secuencial (UI → Code)**
# MAGIC
# MAGIC Genie se *entiende* creándolo en la **UI**: seleccionas tablas, escribes *instructions* en español, defines relaciones (JOINs) y pruebas preguntas hasta que responde bien. Esa intuición de "cómo guiar a Genie" no se construye desde código.
# MAGIC
# MAGIC Una vez tienes un space que funciona, lo llevas a **producción con código**: una **App Streamlit** que abre conversaciones con `w.genie.start_conversation_and_wait(...)` y muestra texto + tabla + SQL generado. La UI **diseña**; el código **entrega** la experiencia al usuario final.
# MAGIC
# MAGIC 📓 Contenido detallado: `labs/genie_y_apps/` (01 Introducción + datos, 02 Crear Genie + preguntas, 03 App Streamlit).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Datos (código)
# MAGIC
# MAGIC Corre `labs/genie_y_apps/01_Introduccion_Apps_y_Genie` para crear la tabla `inventario_insumos_oficina` (con comentarios de tabla y columnas, que Genie aprovecha). Resumen del setup:

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")
spark.sql(f"USE CATALOG `{CATALOG}`"); spark.sql(f"USE SCHEMA `{SCHEMA}`")
print(f"Genie usará tablas de: {CATALOG}.{SCHEMA}")
print("→ Asegúrate de haber corrido labs/genie_y_apps/01 para tener 'inventario_insumos_oficina'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Crear el Genie space en la UI (🖱️)
# MAGIC
# MAGIC 1. **Sidebar → Genie → New** (o SQL Editor → panel Genie).
# MAGIC 2. Nombre: `Genie Inventario — <tu_usuario>`. Fuente: tu tabla `inventario_insumos_oficina`.
# MAGIC 3. **Instructions** (en español):
# MAGIC    ```
# MAGIC    Responde en español. Usa los campos y sus descripciones tal como están en la tabla.
# MAGIC    Si la pregunta es ambigua, pide aclaraciones y sugiere filtros (categoría, fecha).
# MAGIC    Cuando corresponda, sugiere una visualización y limita los resultados.
# MAGIC    No inventes datos fuera de las tablas configuradas.
# MAGIC    ```
# MAGIC 4. **Preguntas de prueba** (escríbelas en el chat):
# MAGIC    - *¿Cuántos ítems hay por categoría y cuál es el stock promedio?*
# MAGIC    - *Top 10 ítems por debajo del stock mínimo.*
# MAGIC    - *Tendencia mensual de compras (serie temporal).*
# MAGIC    - *¿Cuál es la receta de la paella?* (fuera de alcance → debe declinar con elegancia).
# MAGIC 5. Revisa el **SQL generado** en cada respuesta (click *Show generated code*). **Aquí ves que Genie traduce NL→SQL gobernado.**
# MAGIC 6. **Copia el Space ID** desde la URL del space — lo necesita la App.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — La App que consume Genie (código)
# MAGIC
# MAGIC En `labs/genie_y_apps/03_App_Streamlit_Actualizar_Inventario` está la App completa (tabla en vivo, filtros, formulario de actualización de stock, y **chatbot conectado a Genie**). El corazón de la integración:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()
# MAGIC genie_space_id = "PEGA_AQUI_TU_SPACE_ID"
# MAGIC
# MAGIC if prompt := st.chat_input("Pregunta sobre el inventario..."):
# MAGIC     if st.session_state.get("conversation_id"):
# MAGIC         conv = w.genie.create_message_and_wait(genie_space_id, st.session_state.conversation_id, prompt)
# MAGIC     else:
# MAGIC         conv = w.genie.start_conversation_and_wait(genie_space_id, prompt)
# MAGIC     for a in conv.attachments:
# MAGIC         if a.text:  st.markdown(a.text.content)
# MAGIC         elif a.query:
# MAGIC             st.code(a.query.query, language="sql")   # el SQL que generó Genie
# MAGIC ```
# MAGIC
# MAGIC ### Desplegar la App (🖱️ + CLI)
# MAGIC - **UI:** Sidebar → **Compute → Apps → Create app → Custom**, apunta a la carpeta de la App, set el secreto/variable con el `genie_space_id`, **Deploy**.
# MAGIC - **Código:** `databricks apps deploy <app-name> --source-code-path <ruta>` (mismo resultado, para CI/CD).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Creaste un **Genie space** en la UI y lo guiaste con instructions + preguntas de prueba
# MAGIC ✅ Viste el **SQL generado** (NL → SQL gobernado)
# MAGIC ✅ Conectaste una **App Streamlit** al space vía el SDK (`w.genie...`)
# MAGIC ✅ Patrón **UI → Code**: la UI diseña, el código entrega
# MAGIC
# MAGIC ## Continuar → `05 - LAB Agent Bricks (Knowledge Assistant)`
