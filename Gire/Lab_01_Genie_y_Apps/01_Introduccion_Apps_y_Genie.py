# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Taller: Introducción a Apps y Genie en Databricks (Parte 1)
# MAGIC
# MAGIC En este cuaderno:
# MAGIC - Presentaremos Databricks Apps y Genie y cómo trabajan juntos.
# MAGIC - Crearemos un catálogo y esquema dedicados para el taller.
# MAGIC - Generaremos una tabla de inventario (insumos de oficina) con descripciones detalladas (tabla y columnas) que usaremos con Genie.
# MAGIC
# MAGIC Requisitos:
# MAGIC - Ejecutar en un workspace con Unity Catalog habilitado.
# MAGIC - Permisos para crear catálogo/esquema/tabla.
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup del lab
# MAGIC
# MAGIC Catálogo compartido: `ardemo_classic_dnubtw_catalog`. Schema personal por usuario: `ws_<usuario>`.
# MAGIC Esta celda valida acceso y crea tu schema si no existe.

# COMMAND ----------

CATALOG = catalog = CATALOGO = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = db = schema = ESQUEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")
try:
    spark.conf.set("c.catalog", CATALOG)
    spark.conf.set("c.schema", SCHEMA)
except Exception:
    pass  # Not available on Serverless

print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print(f"User:    {_user}")


# Configuración de nombres: usa tu apellido para personalizar el catálogo
# Si no quieres widgets, edita las variables directamente.


# (replaced by setup cell) CATALOGO override removed
# (replaced by setup cell) ESQUEMA override removed
TABLA = "inventario_insumos_oficina"

print(f"Catálogo: {CATALOGO}")
print(f"Esquema:   {ESQUEMA}")
print(f"Tabla:     {TABLA}")

# COMMAND ----------

# Crear catálogo y esquema (si no existen) y usar el catálogo
# (replaced by setup cell)
spark.sql(f"USE CATALOG `{CATALOGO}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOGO}`.`{ESQUEMA}`")
spark.sql(f"USE `{CATALOGO}`.`{ESQUEMA}`")

print("✓ Catálogo y esquema listos")


# COMMAND ----------

# Generar DataFrame de inventario de insumos de oficina con datos sintéticos
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime, timedelta
import random

categorias = [
    ("Escritura", ["Bolígrafo", "Lápiz", "Marcador", "Resaltador"]),
    ("Papel", ["Papel A4", "Notas adhesivas", "Libreta", "Carpetas"]),
    ("Organización", ["Archivador", "Porta-documentos", "Separadores", "Grapadora"]),
    ("Tecnología", ["Mouse", "Teclado", "Cargador", "Hub USB"]),
    ("Limpieza", ["Toallas", "Toallas húmedas", "Desinfectante", "Bolsas de basura"]),
]

proveedores = ["OfiMax", "Papelería Centro", "TechPlus", "Distribuidora Sur", "Norte Office"]
monedas = ["USD", "EUR", "MXN"]
estados = ["activo", "descontinuado", "pendiente_reposición"]
ubicaciones = [
    ("ALM-01", "A", "1", "Bajo"),
    ("ALM-01", "B", "2", "Medio"),
    ("ALM-02", "C", "3", "Alto"),
]

schema = T.StructType([
    T.StructField("item_id", T.StringType()),
    T.StructField("nombre", T.StringType()),
    T.StructField("categoria", T.StringType()),
    T.StructField("subcategoria", T.StringType()),
    T.StructField("descripcion", T.StringType()),
    T.StructField("proveedor", T.StringType()),
    T.StructField("sku", T.StringType()),
    T.StructField("unidad_medida", T.StringType()),
    T.StructField("stock_actual", T.IntegerType()),
    T.StructField("stock_minimo", T.IntegerType()),
    T.StructField("stock_maximo", T.IntegerType()),
    T.StructField("precio_unitario", T.DoubleType()),
    T.StructField("moneda", T.StringType()),
    T.StructField("ubicacion_almacen", T.StringType()),
    T.StructField("pasillo", T.StringType()),
    T.StructField("estante", T.StringType()),
    T.StructField("nivel", T.StringType()),
    T.StructField("fecha_ultima_compra", T.DateType()),
    T.StructField("fecha_ultima_salida", T.DateType()),
    T.StructField("dias_rotacion", T.IntegerType()),
    T.StructField("estado_producto", T.StringType()),
    T.StructField("lote", T.StringType()),
    T.StructField("fecha_caducidad", T.DateType()),
    T.StructField("codigo_barras", T.StringType()),
    T.StructField("codigo_qr", T.StringType()),
    T.StructField("responsable", T.StringType()),
    T.StructField("created_at", T.TimestampType()),
    T.StructField("updated_at", T.TimestampType()),
])

rows = []
base_date = datetime(2024, 1, 1)
for i in range(1, 101):
    cat, subs = random.choice(categorias)
    sub = random.choice(subs)
    proveedor = random.choice(proveedores)
    moneda = random.choice(monedas)
    ubic, pas, est, niv = random.choice(ubicaciones)
    stock_max = random.randint(50, 300)
    stock_min = random.randint(5, 30)
    stock_act = random.randint(0, stock_max)
    precio = round(random.uniform(0.5, 150.0), 2)
    dias_rot = random.randint(1, 120)
    estado = random.choice(estados)
    compra = (base_date + timedelta(days=random.randint(0, 300))).date()
    salida = (compra + timedelta(days=random.randint(0, 60)))
    caducidad = None
    if cat in ("Limpieza",):
        caducidad = (compra + timedelta(days=random.randint(180, 720)))
    now = datetime.utcnow()
    rows.append((
        f"ITM{i:04d}",
        f"{sub} {i}",
        cat,
        sub,
        f"{sub} de categoría {cat} para uso de oficina.",
        proveedor,
        f"SKU-{i:05d}",
        "unidad",
        stock_act,
        stock_min,
        stock_max,
        float(precio),
        moneda,
        ubic,
        pas,
        est,
        niv,
        compra,
        salida,
        dias_rot,
        estado,
        f"L{i:05d}",
        caducidad,
        f"CB{i:013d}",
        f"QR{i:013d}",
        "bodega_central",
        now,
        now,
    ))

df = spark.createDataFrame(rows, schema)

df.write.mode("overwrite").saveAsTable(f"`{CATALOGO}`.`{ESQUEMA}`.`{TABLA}`")
print("✓ Tabla creada:", f"{CATALOGO}.{ESQUEMA}.{TABLA}")


# COMMAND ----------

# Añadir descripciones (tabla y columnas) en español
spark.sql(f"""
COMMENT ON TABLE `{CATALOGO}`.`{ESQUEMA}`.`{TABLA}` IS 
  'Inventario detallado de insumos de oficina: escritura, papel, organización, tecnología y limpieza. Incluye stock, precios, ubicaciones y metadatos operativos.'
""")

col_desc = {
  "item_id": "Identificador único del ítem",
  "nombre": "Nombre comercial del insumo",
  "categoria": "Categoría principal (Ej.: Escritura, Papel, etc.)",
  "subcategoria": "Subcategoría específica (Ej.: Marcador, Resaltador, etc.)",
  "descripcion": "Descripción breve del ítem",
  "proveedor": "Proveedor principal del insumo",
  "sku": "Código SKU interno",
  "unidad_medida": "Unidad de medida (p. ej., unidad, caja, paquete)",
  "stock_actual": "Unidades disponibles actualmente",
  "stock_minimo": "Nivel mínimo recomendado antes de reponer",
  "stock_maximo": "Nivel máximo objetivo",
  "precio_unitario": "Precio por unidad",
  "moneda": "Moneda del precio (USD/EUR/MXN)",
  "ubicacion_almacen": "Código de almacén",
  "pasillo": "Identificador del pasillo",
  "estante": "Identificador del estante",
  "nivel": "Nivel dentro del estante",
  "fecha_ultima_compra": "Fecha de la última compra",
  "fecha_ultima_salida": "Fecha de la última salida/consumo",
  "dias_rotacion": "Velocidad de rotación (días)",
  "estado_producto": "Estado del producto (activo, descontinuado, etc.)",
  "lote": "Identificador de lote si aplica",
  "fecha_caducidad": "Fecha de caducidad si aplica",
  "codigo_barras": "Código de barras",
  "codigo_qr": "Código QR",
  "responsable": "Responsable/área del stock",
  "created_at": "Fecha/hora de creación del registro",
  "updated_at": "Fecha/hora de última actualización",
}

for c, d in col_desc.items():
    spark.sql(f"COMMENT ON COLUMN `{CATALOGO}`.`{ESQUEMA}`.`{TABLA}`.{c} IS '{d}'")

print("✓ Descripciones aplicadas a tabla y columnas")
