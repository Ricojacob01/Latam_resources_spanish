# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — LAB Express 🧪 · Ingesta y Medallion
# MAGIC
# MAGIC **25 min.** Generamos datos de ejemplo, los ingerimos incrementalmente con **Auto Loader** y construimos bronze → silver → gold **en notebook**. Luego lo inspeccionamos en la **UI** (Catalog Explorer).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este módulo — **Secuencial (Code → UI)**
# MAGIC
# MAGIC La ingesta incremental se **entiende mejor escribiendo código**: ves cómo `read_files(... STREAM ...)` solo procesa archivos *nuevos*. Por eso hacemos primero el **código**, y al final abrimos **Catalog Explorer (UI)** para ver las tablas, esquemas, *sample data* y **lineage** que el código produjo. La UI **confirma y gobierna** lo que el código creó.
# MAGIC
# MAGIC > En el módulo `03` damos el salto: el mismo medallion pero como **Spark Declarative Pipeline**, donde Databricks gestiona el flujo por ti.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup del lab

# COMMAND ----------

import json, os
from datetime import datetime, timedelta
import random

CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

# Volume raw para los datos fuente (lo reusa el pipeline del módulo 03)
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.raw_de")
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw_de"
for sub in ["orders", "customers"]:
    os.makedirs(f"{RAW}/{sub}", exist_ok=True)

print(f"Catalog: {CATALOG}\nSchema:  {SCHEMA}\nRaw:     {RAW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Generar datos de ejemplo (archivo 00)
# MAGIC
# MAGIC Creamos un primer lote de **pedidos** (`orders`) y **eventos CDC de clientes** (`customers`). Más adelante agregaremos un segundo archivo para ver el procesamiento incremental.

# COMMAND ----------

def gen_orders(path, file_no, n):
    base = datetime(2024, 1, 1)
    rows = []
    for i in range(n):
        rows.append({
            "order_id": f"ORD{file_no*1000 + i:05d}",
            "order_timestamp": (base + timedelta(days=random.randint(0, 60),
                                                  minutes=random.randint(0, 1440))).isoformat(),
            "customer_id": f"CUST{random.randint(1, 20):04d}",
            "amount": round(random.uniform(10, 900), 2),
        })
    with open(f"{path}/orders/{file_no:02d}.json", "w") as f:
        f.write("\n".join(json.dumps(r) for r in rows))
    return len(rows)

def gen_customers_cdc(path):
    # 27 eventos CDC: 20 INSERT, 5 UPDATE, 2 DELETE
    cities = ["Bogotá", "Medellín", "Cali", "San Francisco"]
    events = []
    ts = 1700000000
    for i in range(1, 21):  # INSERT
        events.append({"customer_id": f"CUST{i:04d}", "operation": "INSERT",
                       "name": f"Cliente {i}", "email": f"cliente{i}@gire.co",
                       "city": random.choice(cities), "timestamp": ts}); ts += 60
    for i in random.sample(range(1, 21), 5):  # UPDATE → ciudad San Francisco
        events.append({"customer_id": f"CUST{i:04d}", "operation": "UPDATE",
                       "name": f"Cliente {i}", "email": f"cliente{i}@gire.co",
                       "city": "San Francisco", "timestamp": ts}); ts += 60
    for i in random.sample(range(1, 21), 2):  # DELETE
        events.append({"customer_id": f"CUST{i:04d}", "operation": "DELETE",
                       "name": None, "email": None, "city": None, "timestamp": ts}); ts += 60
    with open(f"{path}/customers/00.json", "w") as f:
        f.write("\n".join(json.dumps(e) for e in events))
    return len(events)

n_o = gen_orders(RAW, 0, 120)
n_c = gen_customers_cdc(RAW)
print(f"✓ {n_o} pedidos en orders/00.json")
print(f"✓ {n_c} eventos CDC en customers/00.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — 🥉 BRONZE: ingesta incremental con Auto Loader
# MAGIC
# MAGIC `read_files(... )` + `.format("cloudFiles")` (Auto Loader) detecta archivos nuevos automáticamente. Aquí lo usamos en modo batch incremental con checkpoint.

# COMMAND ----------

ckpt = f"{RAW}/_ckpt"

(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", f"{ckpt}/orders_schema")
   .load(f"{RAW}/orders")
   .selectExpr("*", "current_timestamp() AS processing_time", "_metadata.file_name AS source_file")
   .writeStream
   .option("checkpointLocation", f"{ckpt}/orders")
   .trigger(availableNow=True)         # procesa lo disponible y termina
   .toTable("bronze_orders"))

# esperar a que termine el micro-batch availableNow
for q in spark.streams.active:
    q.awaitTermination()

print("bronze_orders:", spark.table("bronze_orders").count(), "filas")
display(spark.table("bronze_orders").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — 🥈 SILVER: limpieza + validación
# MAGIC
# MAGIC Tipamos el timestamp y filtramos filas inválidas (en el módulo 03 esto serán *expectations* declarativas).

# COMMAND ----------

from pyspark.sql import functions as F

silver = (spark.table("bronze_orders")
          .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
          .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
          .filter(F.col("order_timestamp") > F.lit("2020-01-01")))

silver.write.mode("overwrite").saveAsTable("silver_orders_clean")
print("silver_orders_clean:", spark.table("silver_orders_clean").count(), "filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — 🥇 GOLD: agregación de negocio

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_order_summary AS
SELECT date(order_timestamp) AS order_date,
       count(*)               AS total_daily_orders,
       count(DISTINCT customer_id) AS unique_customers,
       round(sum(amount), 2)  AS daily_revenue
FROM silver_orders_clean
GROUP BY date(order_timestamp)
ORDER BY order_date
""")
display(spark.table("gold_order_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Procesamiento incremental (la magia de Auto Loader)
# MAGIC
# MAGIC Agregamos un **segundo archivo** y re-ejecutamos. Auto Loader procesa **solo lo nuevo** — no reprocesa `00.json`.

# COMMAND ----------

n2 = gen_orders(RAW, 1, 60)
print(f"✓ {n2} pedidos nuevos en orders/01.json")

(spark.readStream
   .format("cloudFiles").option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", f"{ckpt}/orders_schema")
   .load(f"{RAW}/orders")
   .selectExpr("*", "current_timestamp() AS processing_time", "_metadata.file_name AS source_file")
   .writeStream.option("checkpointLocation", f"{ckpt}/orders")
   .trigger(availableNow=True).toTable("bronze_orders"))
for q in spark.streams.active:
    q.awaitTermination()

print("bronze_orders ahora:", spark.table("bronze_orders").count(), "filas (120 + 60 = 180)")
print("Archivos procesados:")
display(spark.table("bronze_orders").groupBy("source_file").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — 🖱️ Ahora en la UI: Catalog Explorer
# MAGIC
# MAGIC Vamos a **ver lo que el código creó**. Esto construye la intuición de gobernanza que profundiza el track *Agents and Governance*.
# MAGIC
# MAGIC 1. **Sidebar izquierdo → Catalog**.
# MAGIC 2. Navega a `ardemo_classic_dnubtw_catalog` → tu schema `ws_<usuario>`.
# MAGIC 3. Click en **`gold_order_summary`**:
# MAGIC    - Tab **Overview**: columnas y comentarios.
# MAGIC    - Tab **Sample Data**: preview sin escribir SQL.
# MAGIC    - Tab **Lineage** → **See lineage graph**: verás `bronze_orders → silver_orders_clean → gold_order_summary`. **Este grafo lo dedujo Databricks solo.**
# MAGIC 4. Tab **Permissions**: aquí se haría un `GRANT SELECT` con clicks (lo cubre el track de Governance).
# MAGIC
# MAGIC > 💡 **UI vs Code:** lo que acabas de hacer en notebook (crear tablas, ver datos, entender dependencias) también es 100% navegable y gobernable en la UI. El código **produce**; la UI **explora y gobierna**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Generaste datos raw en un Volume UC
# MAGIC ✅ Ingesta **incremental** con Auto Loader (solo archivos nuevos)
# MAGIC ✅ Medallion bronze → silver → gold en código
# MAGIC ✅ Inspeccionaste tablas + **lineage** en Catalog Explorer (UI)
# MAGIC
# MAGIC **El Volume `raw_de` queda listo** — lo reutiliza el pipeline del módulo `03`.
# MAGIC
# MAGIC ## Continuar → `03 - LAB Spark Declarative Pipeline (Calidad + CDC)`
