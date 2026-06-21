# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — LAB 🧱 · Spark Declarative Pipeline (Calidad + CDC)
# MAGIC
# MAGIC **35 min.** Tomamos el medallion del módulo `02` y lo convertimos en un **Spark Declarative Pipeline (Lakeflow)**: el mismo resultado, pero Databricks gestiona el flujo, el orden, los checkpoints y el procesamiento incremental — y te da un **panel de calidad** y **CDC declarativo**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este módulo — **Code + UI a la vez**
# MAGIC
# MAGIC Un Spark Declarative Pipeline **es código declarativo** (los archivos `.sql` en `pipelines/`), pero su valor pedagógico está en **operarlo en la UI de Pipelines**: el grafo de dependencias, el panel de **expectations** (filas válidas vs. descartadas) y la ejecución incremental.
# MAGIC
# MAGIC Por eso este módulo **combina las dos caras**: lees/editas el **código** (abajo) y lo **creas, ejecutas y monitoreas en la UI**. No es uno *o* el otro — es la forma natural de trabajar con Lakeflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerrequisito
# MAGIC
# MAGIC Haber corrido el módulo `02` (genera el Volume `raw_de` con `orders/` y `customers/`). Esta celda imprime los valores que necesitas en la UI.

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw_de"

print("📋 Valores para configurar el pipeline en la UI:\n")
print(f"  Default catalog : {CATALOG}")
print(f"  Default schema  : {SCHEMA}")
print(f"  Configuración → clave 'source' = {RAW}")
print(f"\n  Código fuente del pipeline (este folder): ./pipelines/orders_pipeline.sql y ./pipelines/customers_pipeline.sql")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte A — El CÓDIGO (revisar)
# MAGIC
# MAGIC Abre `pipelines/orders_pipeline.sql`. Fíjate en tres cosas:
# MAGIC
# MAGIC 1. **`STREAMING TABLE` + `STREAM read_files("${source}/orders")`** → ingesta incremental (Auto Loader). `${source}` es la variable de configuración.
# MAGIC 2. **Expectations** en silver:
# MAGIC    ```sql
# MAGIC    CONSTRAINT valid_order_id  EXPECT (order_id IS NOT NULL)   ON VIOLATION FAIL UPDATE,
# MAGIC    CONSTRAINT valid_amount    EXPECT (amount > 0)             ON VIOLATION DROP ROW
# MAGIC    ```
# MAGIC 3. **`MATERIALIZED VIEW`** en gold → agregación que se refresca incrementalmente.
# MAGIC
# MAGIC Y en `pipelines/customers_pipeline.sql`, el **CDC declarativo**:
# MAGIC ```sql
# MAGIC CREATE FLOW customers_cdc_flow AS
# MAGIC AUTO CDC INTO silver_customers FROM STREAM bronze_customers_clean
# MAGIC   KEYS (customer_id) APPLY AS DELETE WHEN operation = 'DELETE'
# MAGIC   SEQUENCE BY timestamp_datetime STORED AS SCD TYPE 1;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte B — Crear el pipeline en la UI (🖱️)
# MAGIC
# MAGIC 1. **Sidebar → Jobs & Pipelines → Create → ETL Pipeline** (Lakeflow Declarative Pipeline).
# MAGIC 2. **Pipeline name:** `gire_de_<tu_usuario>`.
# MAGIC 3. **Source code:** apunta a la carpeta `pipelines/` de este track
# MAGIC    (`.../Gire/Data Engineering/pipelines`). El pipeline auto-descubre los dos `.sql`.
# MAGIC 4. **Default catalog / schema:** pega los valores que imprimió la celda de arriba
# MAGIC    (`ardemo_classic_dnubtw_catalog` / `ws_<usuario>`).
# MAGIC 5. **Configuration** → **Add configuration**:
# MAGIC    - Key: `source`
# MAGIC    - Value: el path `/Volumes/.../raw_de` que imprimió la celda.
# MAGIC 6. **Serverless:** activado. Click **Create**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte C — Validar y ejecutar (🖱️ UI)
# MAGIC
# MAGIC 1. **Validate** (dry run): comprueba sintaxis y dependencias **sin** materializar datos.
# MAGIC 2. **Start**: corre el pipeline completo. Observa el **grafo** construirse:
# MAGIC
# MAGIC    ```
# MAGIC    bronze_orders ───────────────► silver_orders_clean ──► gold_order_summary
# MAGIC    bronze_customers_raw ─► bronze_customers_clean ─(AUTO CDC)─► silver_customers ──► gold_customer_summary
# MAGIC    ```
# MAGIC 3. Click en **`silver_orders_clean`** → tab **Data quality**: cuántas filas pasaron cada *expectation* y cuántas se **descartaron** (`amount > 0`, etc.). **Este panel es el corazón del módulo.**
# MAGIC 4. Click en **`silver_customers`**: debe tener **18** filas (20 INSERT − 2 DELETE), con 5 clientes en *San Francisco* (los UPDATE).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte D — Verificar resultados en código
# MAGIC
# MAGIC Una vez el pipeline corrió (estado **Completed**), las tablas viven en tu schema. Las consultamos desde aquí — cerrando el círculo Code ↔ UI.

# COMMAND ----------

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

try:
    print("gold_order_summary:")
    display(spark.table("gold_order_summary"))

    activos = spark.table("silver_customers").count()
    print(f"\nsilver_customers (clientes activos, esperado 18): {activos}")
    print("\nClientes movidos a San Francisco (UPDATE aplicado por CDC):")
    display(spark.sql("SELECT customer_id, name, city FROM silver_customers WHERE city = 'San Francisco'"))

    print("\nVerificación: los DELETE ya NO existen en silver_customers")
    display(spark.sql("""
        SELECT customer_id FROM bronze_customers_raw WHERE operation = 'DELETE'
        EXCEPT SELECT customer_id FROM silver_customers
    """))
except Exception as e:
    print("Corre primero el pipeline en la UI (Parte C). Detalle:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte E (opcional) — Procesamiento incremental + BI
# MAGIC
# MAGIC - **Incremental:** vuelve al módulo `02` y corre `gen_orders(RAW, 2, 50)` para dejar un archivo nuevo. Dispara el pipeline otra vez (**Start**) y observa que solo procesa lo nuevo.
# MAGIC - **BI (🖱️):** sobre `gold_order_summary` crea un dashboard AI/BI: **Catalog → tabla → Create → Dashboard**, o desde **SQL Editor** guarda una query y agrégale tiles (counter de pedidos, línea de `daily_revenue` por `order_date`). Habilita **Genie** en el dashboard para preguntar en lenguaje natural (esto enlaza con el track *Agents and Governance*).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Convertiste un medallion imperativo en un **Spark Declarative Pipeline**
# MAGIC ✅ Viste el **grafo** de dependencias inferido automáticamente
# MAGIC ✅ Monitoreaste **calidad de datos** (expectations) en la UI
# MAGIC ✅ Aplicaste **CDC** con `AUTO CDC INTO` (SCD Tipo 1) sin escribir MERGE
# MAGIC ✅ Combinaste **código** (definición) + **UI** (creación, ejecución, monitoreo)
# MAGIC
# MAGIC ## Continuar → `04 - LAB Orquestacion con Jobs`
