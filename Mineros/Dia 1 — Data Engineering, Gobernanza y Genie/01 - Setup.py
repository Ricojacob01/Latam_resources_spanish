# Databricks notebook source
# MAGIC %md
# MAGIC # Configuración del Taller - Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC Este cuaderno configura el entorno para el taller de 90 minutos de Lakeflow Spark Declarative Pipelines.
# MAGIC
# MAGIC **Ejecuta este cuaderno UNA VEZ al inicio del taller.**
# MAGIC
# MAGIC ## Qué crea esta configuración:
# MAGIC
# MAGIC 1. **Catálogo** - Catálogo específico por usuario (sdp_workshop_<username>)
# MAGIC 2. **Esquemas** - Esquemas Bronze, Silver y Gold para arquitectura medallion
# MAGIC 3. **Volumen Raw** - Un Volumen UC para aterrizar archivos de datos fuente sin procesar
# MAGIC 4. **Datos de ejemplo** - Archivos JSON iniciales para pedidos, estados y clientes
# MAGIC
# MAGIC ## Estructura del taller:
# MAGIC
# MAGIC - **Ejercicio 1** (40 min): Construir un pipeline simple con datos de pedidos
# MAGIC - **Ejercicio 2** (50 min): Agregar CDC de clientes y programar para producción

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1: Inicializar el entorno del taller

# COMMAND ----------

import re

# Obtener información del usuario actual
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username = current_user.split("@")[0]

# Limpiar el nombre de usuario para su uso en nombres (eliminar caracteres especiales)
clean_username = re.sub(r'[^a-z0-9]', '_', username.lower())

# Crear una clase de ayuda
class WorkshopHelper:
    def __init__(self):
        self.username = username
        self.clean_username = clean_username
        self.catalog_name = f"sdp_workshop_{clean_username}"  # Catálogo con nombre de usuario
        self.default_schema = "default"  # Para almacenamiento de volúmenes
        
        # Definir rutas
        self.working_dir = f"/Volumes/{self.catalog_name}/{self.default_schema}/raw"
        
        # Nombres de esquemas - arquitectura medallion simple
        self.bronze_schema = "bronze"
        self.silver_schema = "silver"
        self.gold_schema = "gold"
    
    def print_config(self):
        print(f"""
Configuración del Taller
=====================
Usuario: {self.username}
Catálogo: {self.catalog_name}
Directorio de trabajo: {self.working_dir}

Esquemas:
- Bronze: {self.catalog_name}.{self.bronze_schema}
- Silver: {self.catalog_name}.{self.silver_schema}
- Gold: {self.catalog_name}.{self.gold_schema}
        """)

# Inicializar el helper
DA = WorkshopHelper()
DA.print_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3: Crear catálogo y esquemas

# COMMAND ----------

# Crear el catálogo primero
spark.sql(f"CREATE CATALOG IF NOT EXISTS {DA.catalog_name}")
print(f"✓ Catálogo creado: {DA.catalog_name}")

# Crear el esquema por defecto para volúmenes
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DA.catalog_name}.{DA.default_schema}")
print(f"✓ Esquema creado: {DA.catalog_name}.{DA.default_schema}")

# Crear los tres esquemas para la arquitectura medallion
schemas_to_create = [DA.bronze_schema, DA.silver_schema, DA.gold_schema]

for schema in schemas_to_create:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DA.catalog_name}.{schema}")
    print(f"✓ Esquema creado: {DA.catalog_name}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4: Crear volumen Raw para los datos fuente

# COMMAND ----------

# Crear volumen en el esquema por defecto para archivos fuente sin procesar
volume_name = "raw"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {DA.catalog_name}.{DA.default_schema}.{volume_name}")
print(f"✓ Volumen creado: {DA.catalog_name}.{DA.default_schema}.{volume_name}")
print(f"  Ruta: {DA.working_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5: Crear directorios para datos fuente sin procesar

# COMMAND ----------

# Crear directorios para datos fuente
dbutils.fs.mkdirs(f"{DA.working_dir}/orders")
dbutils.fs.mkdirs(f"{DA.working_dir}/status")
dbutils.fs.mkdirs(f"{DA.working_dir}/customers")

print("✓ Directorios de datos fuente sin procesar creados:")
print(f"  - {DA.working_dir}/orders")
print(f"  - {DA.working_dir}/status")
print(f"  - {DA.working_dir}/customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6: Generar datos de ejemplo de pedidos

# COMMAND ----------

import json
from datetime import datetime, timedelta
import random

# Generar pedidos de ejemplo
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
    
    # Escribir al volumen
    file_path = f"{DA.working_dir}/orders/{file_name}"
    dbutils.fs.put(file_path, "\n".join([json.dumps(order) for order in orders]), overwrite=True)
    
    return len(orders)

# Generar archivo inicial de pedidos
num_orders = generate_orders(num_orders=174, file_name="00.json")
print(f"✓ Se generaron {num_orders} pedidos de ejemplo en 00.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7: Generar datos de ejemplo de estados

# COMMAND ----------

def generate_status_updates(num_updates=536, file_name="00.json"):
    """Generar actualizaciones de estado de pedidos de ejemplo"""
    # Nota: se mantienen los valores de estado en inglés para evitar romper ejercicios posteriores
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
    
    # Escribir al volumen
    file_path = f"{DA.working_dir}/status/{file_name}"
    dbutils.fs.put(file_path, "\n".join([json.dumps(update) for update in status_updates]), overwrite=True)
    
    return len(status_updates)

# Generar archivo inicial de estados
num_status = generate_status_updates(num_updates=536, file_name="00.json")
print(f"✓ Se generaron {num_status} actualizaciones de estado de ejemplo en 00.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 8: Generar datos de ejemplo de CDC de clientes

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
    
    # Escribir al volumen
    file_path = f"{DA.working_dir}/customers/{file_name}"
    dbutils.fs.put(file_path, "\n".join([json.dumps(c) for c in customers]), overwrite=True)
    
    return len(customers)

# Generar archivo inicial de CDC de clientes
num_customers = generate_customer_cdc(file_name="00.json")
print(f"✓ Se generaron {num_customers} eventos CDC de clientes en 00.json")
print(f"  - 20 operaciones INSERT")
print(f"  - 5 operaciones UPDATE")
print(f"  - 2 operaciones DELETE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 9: ¡Configuración completa!

# COMMAND ----------

catalog = DA.catalog_name
working_dir = DA.working_dir

print(f"""
================================================================================
                    ¡CONFIGURACIÓN DEL TALLER COMPLETA! ✓
================================================================================

IMPORTANTE: Guarda estos valores para la configuración de tu pipeline:

1. Catálogo predeterminado: {catalog}
2. Esquema predeterminado: bronze
3. Variable de configuración:
     Clave: source
     Valor: {working_dir}

Zona de aterrizaje de datos sin procesar:
  {working_dir}

Esquemas creados:
  • {catalog}.bronze
  • {catalog}.silver
  • {catalog}.gold

Datos sin procesar de ejemplo creados:
  • 174 pedidos en orders/00.json
  • 536 actualizaciones de estado en status/00.json
  • 27 eventos CDC de clientes en customers/00.json

--------------------------------------------------------------------------------
Siguientes pasos:
  1. Abre "Exercise_1/1-Building_Pipelines_with_Data_Quality.sql"
  2. Crea tu primer pipeline
================================================================================
""")

