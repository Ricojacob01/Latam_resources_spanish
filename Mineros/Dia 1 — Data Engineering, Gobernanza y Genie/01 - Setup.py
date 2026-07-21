# Databricks notebook source
# MAGIC %md
# MAGIC # Día 1 · Lección 1: Configuración del Taller
# MAGIC
# MAGIC Este cuaderno configura tu entorno para el taller de Lakeflow Spark Declarative Pipelines.
# MAGIC
# MAGIC **Ejecuta este cuaderno UNA VEZ al inicio del Día 1.**
# MAGIC
# MAGIC ## Modelo de aislamiento: catálogo compartido + esquema por usuario
# MAGIC - Todos trabajamos en el **catálogo compartido `academia`**.
# MAGIC - Cada participante tiene **su propio esquema** `academia.<tu_apellido>`.
# MAGIC - La capa medallion (Bronze/Silver/Gold) se distingue por el **sufijo del nombre de la tabla**:
# MAGIC   `orders_bronze`, `orders_silver`, `order_summary_gold`, etc.
# MAGIC
# MAGIC ## Qué crea esta configuración:
# MAGIC 1. **Esquema** por usuario dentro de `academia`.
# MAGIC 2. **Volumen Raw** (`academia.<usuario>.raw`) para aterrizar los archivos fuente.
# MAGIC 3. **Datos de ejemplo** (JSON) para pedidos, estados y eventos CDC de clientes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1: Inicializar el entorno del taller

# COMMAND ----------

import re

# Obtener información del usuario actual
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username = current_user.split("@")[0]

# Limpiar el nombre de usuario para usarlo como nombre de esquema (solo minúsculas/números/_)
clean_username = re.sub(r'[^a-z0-9]', '_', username.lower())

# Clase de ayuda
class WorkshopHelper:
    def __init__(self):
        self.username = username
        self.clean_username = clean_username
        self.catalog_name = "academia"            # Catálogo COMPARTIDO
        self.schema_name = clean_username          # Esquema PROPIO de cada usuario
        self.volume_name = "raw"

        # Ruta del volumen: /Volumes/academia/<usuario>/raw
        self.working_dir = f"/Volumes/{self.catalog_name}/{self.schema_name}/{self.volume_name}"

    def print_config(self):
        print(f"""
Configuración del Taller
=====================
Usuario:  {self.username}
Catálogo: {self.catalog_name}   (compartido)
Esquema:  {self.schema_name}    (tuyo)
Volumen:  {self.catalog_name}.{self.schema_name}.{self.volume_name}
Ruta:     {self.working_dir}

Convención de tablas (arquitectura medallion en un solo esquema):
  Bronze → orders_bronze, customers_raw, customers_clean
  Silver → orders_silver, customers_silver
  Gold   → order_summary_gold, customer_summary_gold
        """)

DA = WorkshopHelper()
DA.print_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2: Crear tu esquema en el catálogo compartido
# MAGIC No creamos catálogo: `academia` ya existe y es compartido. Solo creamos **tu** esquema.

# COMMAND ----------

# Usar el catálogo compartido
spark.sql(f"USE CATALOG {DA.catalog_name}")

# Crear el esquema propio del usuario (idempotente)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DA.catalog_name}.{DA.schema_name}")
print(f"✓ Esquema listo: {DA.catalog_name}.{DA.schema_name}")

# Establecerlo como esquema por defecto para el resto del notebook
spark.sql(f"USE SCHEMA {DA.schema_name}")
print(f"✓ Contexto por defecto: {DA.catalog_name}.{DA.schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3: Crear el volumen Raw para los datos fuente

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {DA.catalog_name}.{DA.schema_name}.{DA.volume_name}")
print(f"✓ Volumen creado: {DA.catalog_name}.{DA.schema_name}.{DA.volume_name}")
print(f"  Ruta: {DA.working_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4: Crear directorios para datos fuente sin procesar

# COMMAND ----------

dbutils.fs.mkdirs(f"{DA.working_dir}/orders")
dbutils.fs.mkdirs(f"{DA.working_dir}/status")
dbutils.fs.mkdirs(f"{DA.working_dir}/customers")

print("✓ Directorios creados:")
print(f"  - {DA.working_dir}/orders")
print(f"  - {DA.working_dir}/status")
print(f"  - {DA.working_dir}/customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5: Generar datos de ejemplo de pedidos

# COMMAND ----------

import json
from datetime import datetime, timedelta
import random

def generate_orders(num_orders=174, file_name="00.json"):
    """Generar datos de pedidos de ejemplo"""
    orders = []
    base_date = datetime(2024, 1, 1)

    for i in range(num_orders):
        order = {
            "order_id": f"ORD{i+1000:05d}",
            "order_timestamp": (base_date + timedelta(days=random.randint(0, 30))).isoformat(),
            "customer_id": f"CUST{random.randint(1, 100):04d}",
            "notifications": {
                "email": random.choice([True, False]),
                "sms": random.choice([True, False])
            }
        }
        orders.append(order)

    file_path = f"{DA.working_dir}/orders/{file_name}"
    dbutils.fs.put(file_path, "\n".join([json.dumps(order) for order in orders]), overwrite=True)
    return len(orders)

num_orders = generate_orders(num_orders=174, file_name="00.json")
print(f"✓ Se generaron {num_orders} pedidos de ejemplo en 00.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6: Generar datos de ejemplo de estados

# COMMAND ----------

def generate_status_updates(num_updates=536, file_name="00.json"):
    """Generar actualizaciones de estado de pedidos de ejemplo"""
    # Nota: los valores de estado se mantienen en inglés para no romper ejercicios posteriores
    statuses = ['placed', 'preparing', 'on the way', 'delivered', 'canceled']
    status_updates = []

    base_timestamp = datetime(2024, 1, 1).timestamp()

    for i in range(num_updates):
        update = {
            "order_id": f"ORD{random.randint(1000, 1173):05d}",
            "order_status": random.choice(statuses),
            "status_timestamp": base_timestamp + (i * 3600)  # Marca de tiempo Unix
        }
        status_updates.append(update)

    file_path = f"{DA.working_dir}/status/{file_name}"
    dbutils.fs.put(file_path, "\n".join([json.dumps(update) for update in status_updates]), overwrite=True)
    return len(status_updates)

num_status = generate_status_updates(num_updates=536, file_name="00.json")
print(f"✓ Se generaron {num_status} actualizaciones de estado de ejemplo en 00.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7: Generar datos de ejemplo de CDC de clientes

# COMMAND ----------

def generate_customer_cdc(file_name="00.json"):
    """Generar eventos CDC de clientes de ejemplo"""
    customers = []
    base_timestamp = datetime(2024, 1, 1).timestamp()

    # Operaciones INSERT - 20 clientes nuevos
    for i in range(1, 21):
        customer = {
            "customer_id": f"CUST{i:04d}",
            "name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "address": f"{i*100} Main St",
            "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston"]),
            "state": random.choice(["NY", "CA", "IL", "TX"]),
            "zip_code": f"{10000 + i:05d}",
            "operation": "INSERT",
            "timestamp": base_timestamp + (i * 1000)
        }
        customers.append(customer)

    # Operaciones UPDATE - 5 clientes cambian email/dirección
    for i in [1, 5, 10, 15, 20]:
        customer = {
            "customer_id": f"CUST{i:04d}",
            "name": f"Customer {i}",
            "email": f"newemail{i}@example.com",  # Email cambiado
            "address": f"{i*200} Oak Ave",  # Dirección cambiada
            "city": "San Francisco",  # Ciudad cambiada
            "state": "CA",
            "zip_code": f"{94000 + i:05d}",
            "operation": "UPDATE",
            "timestamp": base_timestamp + (30 * 1000) + (i * 100)  # Tiempos posteriores
        }
        customers.append(customer)

    # Operaciones DELETE - 2 clientes eliminados
    for i in [3, 7]:
        customer = {
            "customer_id": f"CUST{i:04d}",
            "operation": "DELETE",
            "timestamp": base_timestamp + (60 * 1000) + (i * 100)  # Aún más tarde
        }
        customers.append(customer)

    file_path = f"{DA.working_dir}/customers/{file_name}"
    dbutils.fs.put(file_path, "\n".join([json.dumps(c) for c in customers]), overwrite=True)
    return len(customers)

num_customers = generate_customer_cdc(file_name="00.json")
print(f"✓ Se generaron {num_customers} eventos CDC de clientes en 00.json")
print(f"  - 20 operaciones INSERT")
print(f"  - 5 operaciones UPDATE")
print(f"  - 2 operaciones DELETE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 8: ¡Configuración completa!

# COMMAND ----------

print(f"""
================================================================================
                    ¡CONFIGURACIÓN DEL TALLER COMPLETA! ✓
================================================================================

IMPORTANTE: Guarda estos valores para configurar tu pipeline (Lección 2):

1. Catálogo por defecto (Default catalog): {DA.catalog_name}
2. Esquema por defecto (Default schema):   {DA.schema_name}
3. Variable de configuración del pipeline:
     Clave:  source
     Valor:  {DA.working_dir}

Zona de aterrizaje de datos sin procesar:
  {DA.working_dir}

Tu esquema:
  • {DA.catalog_name}.{DA.schema_name}

Datos de ejemplo creados:
  • 174 pedidos en orders/00.json
  • 536 actualizaciones de estado en status/00.json
  • 27 eventos CDC de clientes en customers/00.json

--------------------------------------------------------------------------------
Siguiente paso:
  Abre "02 - Lab Pipeline con Calidad de Datos" y crea tu primer pipeline.
================================================================================
""")
