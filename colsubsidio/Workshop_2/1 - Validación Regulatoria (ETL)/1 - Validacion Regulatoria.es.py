# Databricks notebook source
# MAGIC %md
# MAGIC # 1 · Validación Regulatoria (ETL)
# MAGIC ### Colsubsidio — Optimización del proceso de validación y reporte al ente de control
# MAGIC
# MAGIC El área de **Planeación** recibe mensualmente **4 bases** desde Afiliaciones, las valida con reglas de
# MAGIC negocio, genera el **XML regulatorio** y alimenta tableros de seguimiento. Hoy el proceso tarda
# MAGIC **~40 minutos**; el objetivo es bajarlo a **~10 minutos** y habilitar **comparaciones históricas**.
# MAGIC
# MAGIC ### Qué construimos en este módulo
# MAGIC
# MAGIC | Paso | Acción | Capacidad Databricks |
# MAGIC |------|--------|----------------------|
# MAGIC | **1** | Leer las 4 bases desde SAP HANA | JDBC / Databricks Connection |
# MAGIC | **2** | Ejecutar el catálogo de validaciones | Spark SQL + DataFrames (en memoria) |
# MAGIC | **3** | Consolidar el **informe de inconsistencias** | `union` + agregaciones |
# MAGIC | **4** | Comparación histórica (cambios de documento) | join contra periodo anterior |
# MAGIC | **5** | Generar el **XML regulatorio** | `to_xml` / construcción de string |
# MAGIC | **6** | Escribir resultados **de vuelta a SAP HANA** | JDBC write |
# MAGIC
# MAGIC > **Principio:** el procesamiento es **transitorio** (DataFrames y vistas temporales). **No creamos
# MAGIC > tablas de negocio en Databricks** — el informe de inconsistencias y el XML se escriben de vuelta a
# MAGIC > SAP HANA. En el laboratorio leemos de las tablas sintéticas que generó el notebook `0 - SETUP`.

# COMMAND ----------

# DBTITLE 1,Parámetros — catálogo y esquema del usuario
# ─────────────────────────────────────────────────────────────────────────────
# Parámetros del workshop
# Cada usuario tiene su propio esquema derivado de su correo electrónico.
# Esto permite que múltiples participantes trabajen sin conflictos.
# ─────────────────────────────────────────────────────────────────────────────
dbutils.widgets.text("catalogo", "classic_stable_paco_catalog", "Catálogo compartido")

CATALOGO = dbutils.widgets.get("catalogo").strip()
usuario  = spark.sql("SELECT current_user()").collect()[0][0]
SUFIJO   = usuario.split("@")[0].replace(".", "_").replace("-", "_")
ESQUEMA  = f"ws2_{SUFIJO}"
FQN      = f"{CATALOGO}.{ESQUEMA}"

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")
print(f"✔ Usuario: {usuario}")
print(f"✔ Esquema de trabajo: {FQN}")

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %md
# MAGIC ## 1. Conectar y leer las bases desde SAP HANA
# MAGIC
# MAGIC Cada participante usa la **Databricks Connection** `sap_bw_workshop` registrada en Unity Catalog para
# MAGIC conectarse a SAP HANA — **sin credenciales en el notebook**. La autenticación la maneja Unity Catalog.
# MAGIC
# MAGIC ### Flujo:
# MAGIC 1. **Descubrimiento** — listar las tablas disponibles en el esquema HANA
# MAGIC 2. **Lectura** — leer las 4 bases principales como DataFrames
# MAGIC 3. **Normalización** — convertir nombres de columnas a minúsculas (HANA usa MAYÚSCULAS)
# MAGIC 4. **Vistas temporales** — registrar para uso en SQL declarativo (no persisten en Databricks)
# MAGIC
# MAGIC ### Patrón de lectura:
# MAGIC ```python
# MAGIC df = (spark.read.format("jdbc")
# MAGIC     .option("connectionName", "sap_bw_workshop")
# MAGIC     .option("dbtable", '"WORKSHOP"."EMPRESAS"')
# MAGIC     .load())
# MAGIC ```
# MAGIC
# MAGIC > **Principio:** los datos **viven en SAP HANA**. Databricks solo los lee, procesa en memoria,
# MAGIC > y escribe los resultados de vuelta. No creamos tablas de negocio en Databricks.

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Lectura de las bases (laboratorio vs SAP HANA)
# ─────────────────────────────────────────────────────────────────────────────
# Paso 1: Descubrimiento — ver qué tablas existen en el esquema HANA
# Consultamos el catálogo del sistema (SYS.TABLES) para listar las tablas.
# ─────────────────────────────────────────────────────────────────────────────
CONNECTION_NAME = "sap_bw_workshop"
SAP_SCHEMA      = "WORKSHOP"

# Listar las tablas disponibles en el esquema de la conexión SAP HANA
tablas_disponibles = (spark.read.format("jdbc")
    .option("connectionName", CONNECTION_NAME)
    .option("query", f"SELECT TABLE_NAME, TABLE_TYPE, RECORD_COUNT FROM SYS.TABLES WHERE SCHEMA_NAME = '{SAP_SCHEMA}'")
    .load())

print(f"✔ Tablas disponibles en la conexión '{CONNECTION_NAME}' → esquema {SAP_SCHEMA}:")
display(tablas_disponibles)

# COMMAND ----------

# DBTITLE 1,Leer las 4 bases desde SAP HANA vía Databricks Connection
# ─────────────────────────────────────────────────────────────────────────────
# Lectura de las 4 bases desde SAP HANA usando la Databricks Connection
# Cada participante usa la MISMA conexión registrada en Unity Catalog.
# No se requieren credenciales en el notebook — la autenticación es transparente.
# ─────────────────────────────────────────────────────────────────────────────

def leer_desde_hana(tabla: str) -> "DataFrame":
    """Lee una tabla del esquema SAP HANA usando la conexión registrada."""
    df = (spark.read.format("jdbc")
          .option("connectionName", CONNECTION_NAME)
          .option("dbtable", f'"{SAP_SCHEMA}"."{tabla}"')
          .load())
    # SAP HANA devuelve nombres en MAYÚSCULAS — normalizamos a minúsculas
    return df.toDF(*[c.lower() for c in df.columns])

# Lectura de las bases principales
empresas       = leer_desde_hana("EMPRESAS")
afiliados      = leer_desde_hana("AFILIADOS")
personas_cargo = leer_desde_hana("PERSONAS_CARGO")
aportes        = leer_desde_hana("APORTES")

# Crear vistas temporales para usar SQL declarativo en las validaciones
# (viven solo en esta sesión Spark — NO se persisten en Databricks)
empresas.createOrReplaceTempView("empresas")
afiliados.createOrReplaceTempView("afiliados")
personas_cargo.createOrReplaceTempView("personas_cargo")
aportes.createOrReplaceTempView("aportes")

print(f"✔ empresas       : {empresas.count():,} filas")
print(f"✔ afiliados      : {afiliados.count():,} filas")
print(f"✔ personas_cargo : {personas_cargo.count():,} filas")
print(f"✔ aportes        : {aportes.count():,} filas")
print(f"\nℹ Datos leídos desde SAP HANA ({CONNECTION_NAME}) — esquema {SAP_SCHEMA}")
print("ℹ Las 4 bases están como vistas temporales — no se persistió ninguna tabla en Databricks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Catálogo de validaciones
# MAGIC
# MAGIC Cada validación produce un registro con la estructura del **informe regulatorio**:
# MAGIC
# MAGIC | Columna | Descripción |
# MAGIC |---------|-------------|
# MAGIC | `informe` | Base sobre la que corre la validación |
# MAGIC | `categoria` | Tipo de validación (CALIDAD, FORMATO, CRUCE, HISTORICO) |
# MAGIC | `codigo_validacion` | Código único de la regla |
# MAGIC | `validacion` | Descripción legible de la regla |
# MAGIC | `inconsistencias` | Cantidad de registros que la incumplen |
# MAGIC
# MAGIC Definimos cada regla como un `SELECT` que **cuenta** las filas que la incumplen. Todo corre en memoria.

# COMMAND ----------

# DBTITLE 1,Reglas de validación (SQL declarativo)
# ─────────────────────────────────────────────────────────────────────────────
# Catálogo de reglas de validación regulatoria.
# Cada regla es un SELECT COUNT(*) que corre contra las vistas temporales
# (leídas de SAP HANA). El resultado es la cantidad de INCUMPLIMIENTOS.
# Para agregar nuevas reglas, solo añada tuplas a esta lista.
# ─────────────────────────────────────────────────────────────────────────────
# Cada tupla: (informe, categoria, codigo, descripción, SQL que cuenta incumplimientos)
REGLAS = [
    # ── FORMATO ───────────────────────────────────────────────────────────────
    ("EMPRESAS", "FORMATO", "F01",
     "Formato de correo electrónico no corresponde a nombre@dominio.com",
     "SELECT COUNT(*) FROM empresas WHERE email NOT RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[a-zA-Z]{2,}$'"),
    ("AFILIADOS", "FORMATO", "F02",
     "Longitud del número de identificación vs tipo de identificación",
     "SELECT COUNT(*) FROM afiliados WHERE (tipo_documento IN ('CC','CE') AND length(numero_documento) < 8)"),

    # ── CALIDAD ───────────────────────────────────────────────────────────────
    ("EMPRESAS", "CALIDAD", "C01",
     "Aportes inferiores a 100.000 mil pesos",
     "SELECT COUNT(*) FROM empresas WHERE valor_aporte_mensual < 100000"),
    ("EMPRESAS", "CALIDAD", "C02",
     "Salario/Aporte igual o menor que 0",
     "SELECT COUNT(*) FROM empresas WHERE valor_aporte_mensual <= 0"),
    ("AFILIADOS", "CALIDAD", "C03",
     "Salario del afiliado superior a 100 Millones de Pesos",
     "SELECT COUNT(*) FROM afiliados WHERE salario > 100000000"),
    ("AFILIADOS", "CALIDAD", "C04",
     "Salario igual o menor que 0",
     "SELECT COUNT(*) FROM afiliados WHERE salario <= 0"),
    ("APORTES", "CALIDAD", "C05",
     "Aportantes voluntarios con aportes, estado retirado o sin relación con afiliados",
     "SELECT COUNT(*) FROM aportes WHERE estado = 'VOLUNTARIO' AND valor > 0"),
    ("AFILIADOS", "CALIDAD", "C06",
     "Afiliados Tipo 13 categoría diferente a C (servicio doméstico)",
     "SELECT COUNT(*) FROM afiliados WHERE tipo_afiliado = 13 AND categoria <> 'C'"),
    ("EMPRESAS", "CALIDAD", "C07",
     "Código de municipio donde labora diferente a Cundinamarca y Bogotá",
     "SELECT COUNT(*) FROM empresas WHERE municipio_codigo NOT RLIKE '^(11|25)'"),
    ("APORTES", "CALIDAD", "C08",
     "Periodo de aporte en el futuro",
     "SELECT COUNT(*) FROM aportes WHERE periodo > date_format(current_date(), 'yyyyMM')"),

    # ── CRUCE ENTRE BASES ─────────────────────────────────────────────────────
    ("AFILIADOS", "CRUCE", "X01",
     "Afiliado con NIT de empresa inexistente (huérfano)",
     "SELECT COUNT(*) FROM afiliados a LEFT JOIN empresas e ON a.empresa_nit = e.nit WHERE e.nit IS NULL"),
    ("PERSONAS_CARGO", "CRUCE", "X02",
     "Persona a cargo sin afiliado principal (huérfano)",
     "SELECT COUNT(*) FROM personas_cargo p LEFT JOIN afiliados a ON p.afiliado_id = a.afiliado_id WHERE a.afiliado_id IS NULL"),
    ("APORTES", "CRUCE", "X03",
     "Aporte con NIT de empresa inexistente",
     "SELECT COUNT(*) FROM aportes ap LEFT JOIN empresas e ON ap.nit_empresa = e.nit WHERE e.nit IS NULL"),

    # ── DUPLICADOS ────────────────────────────────────────────────────────────
    ("AFILIADOS", "CALIDAD", "D01",
     "Duplicados por Tipo y Número de documento",
     "SELECT COALESCE(SUM(c-1),0) FROM (SELECT COUNT(*) c FROM afiliados GROUP BY tipo_documento, numero_documento HAVING COUNT(*) > 1)"),

    # ── REGLAS DE PERSONAS A CARGO ────────────────────────────────────────────
    ("PERSONAS_CARGO", "CALIDAD", "P01",
     "Persona a cargo con más de 100 años de edad",
     "SELECT COUNT(*) FROM personas_cargo WHERE floor(datediff(current_date(), fecha_nacimiento)/365.25) > 100"),
    ("PERSONAS_CARGO", "CALIDAD", "P02",
     "Hijos mayores de 18 años que reciben cuota monetaria",
     "SELECT COUNT(*) FROM personas_cargo WHERE parentesco='HIJO' AND floor(datediff(current_date(), fecha_nacimiento)/365.25) > 18 AND recibe_cuota_monetaria='SI'"),
    ("PERSONAS_CARGO", "CALIDAD", "P03",
     "Menores de edad con Tipo de Documento Cédula de Ciudadanía",
     "SELECT COUNT(*) FROM personas_cargo WHERE floor(datediff(current_date(), fecha_nacimiento)/365.25) < 18 AND tipo_documento='CC'"),
]
print(f"✔ {len(REGLAS)} reglas de validación definidas")

# COMMAND ----------

# DBTITLE 1,Ejecutar las reglas y construir el informe de inconsistencias
from pyspark.sql import Row, functions as F

# ─────────────────────────────────────────────────────────────────────────────
# Lectura desde el catálogo (classic_stable_paco_catalog.ws2_rico_martinez)
# en lugar de SAP HANA vía JDBC. Creamos vistas temporales para que las REGLAS
# (definidas con nombres sin prefijo) funcionen sin modificación.
# ─────────────────────────────────────────────────────────────────────────────
empresas       = spark.table(f"{FQN}.sap_empresas")
afiliados      = spark.table(f"{FQN}.sap_afiliados")
personas_cargo = spark.table(f"{FQN}.sap_personas_cargo")
aportes        = spark.table(f"{FQN}.sap_aportes")

empresas.createOrReplaceTempView("empresas")
afiliados.createOrReplaceTempView("afiliados")
personas_cargo.createOrReplaceTempView("personas_cargo")
aportes.createOrReplaceTempView("aportes")

print(f"✔ Datos leídos desde {FQN} (catálogo Unity Catalog)")
print(f"  empresas       : {empresas.count():,} filas")
print(f"  afiliados      : {afiliados.count():,} filas")
print(f"  personas_cargo : {personas_cargo.count():,} filas")
print(f"  aportes        : {aportes.count():,} filas")

# Ejecutar cada regla de validación contra las vistas temporales.
# Cada regla es un SELECT COUNT(*) que cuenta los registros que INCUMPLEN la regla.
filas = []
for informe, categoria, codigo, descripcion, sql in REGLAS:
    n = spark.sql(sql).collect()[0][0]
    filas.append(Row(informe=informe, categoria=categoria, codigo_validacion=codigo,
                     validacion=descripcion, inconsistencias=int(n)))

informe_inconsistencias = (
    spark.createDataFrame(filas)
    .withColumn("estado", F.when(F.col("inconsistencias") == 0, F.lit("OK")).otherwise(F.lit("REVISAR")))
    .withColumn("fecha_validacion", F.current_date())
    .withColumn("procesado_por", F.lit("DATABRICKS"))
    .orderBy(F.col("inconsistencias").desc())
)
informe_inconsistencias.createOrReplaceTempView("informe_inconsistencias")

total = informe_inconsistencias.agg(F.sum("inconsistencias")).collect()[0][0]
print(f"✔ Inconsistencias totales detectadas: {total:,}")
display(informe_inconsistencias)

# COMMAND ----------

# DBTITLE 1,Resumen por categoría
display(
    informe_inconsistencias
    .groupBy("categoria")
    .agg(F.sum("inconsistencias").alias("inconsistencias"),
         F.count("*").alias("num_reglas"))
    .orderBy(F.col("inconsistencias").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 Ejercicio 1 — Añade una regla de validación
# MAGIC
# MAGIC Agrega una nueva regla al catálogo `REGLAS` que detecte **empresas en estado `MORA` que aún registran
# MAGIC aportes con estado `PAGADO`** (cruce `empresas` ↔ `aportes`). Vuelve a ejecutar las dos celdas
# MAGIC anteriores y verifica que aparezca en el informe.
# MAGIC
# MAGIC <details><summary>💡 Pista</summary>
# MAGIC
# MAGIC ```python
# MAGIC ("APORTES", "CRUCE", "X04",
# MAGIC  "Empresa en MORA con aportes en estado PAGADO",
# MAGIC  "SELECT COUNT(*) FROM aportes ap JOIN empresas e ON ap.nit_empresa=e.nit "
# MAGIC  "WHERE e.estado='MORA' AND ap.estado='PAGADO'"),
# MAGIC ```
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Comparación histórica — cambios de documento entre periodos
# MAGIC
# MAGIC Una capacidad que hoy **no es viable** por el volumen: comparar el periodo actual contra el anterior
# MAGIC para detectar afiliados que **cambiaron de número de documento** (posible corrección o fraude).
# MAGIC En Databricks es un simple `join`.

# COMMAND ----------

# DBTITLE 1,Detectar cambios de documento de identidad
# Leer la base del periodo anterior desde el catálogo Unity Catalog
periodo_anterior = spark.table(f"{FQN}.sap_afiliados_periodo_anterior")
periodo_anterior.createOrReplaceTempView("afiliados_periodo_anterior")

cambios_documento = spark.sql("""
  SELECT
    a.afiliado_id,
    a.nombre,
    p.numero_documento AS documento_anterior,
    a.numero_documento AS documento_actual,
    a.tipo_documento
  FROM afiliados a
  JOIN afiliados_periodo_anterior p ON a.afiliado_id = p.afiliado_id
  WHERE a.numero_documento <> p.numero_documento
""")
cambios_documento.createOrReplaceTempView("cambios_documento")

# Retiros: estaban el periodo anterior y ya no están
retirados = spark.sql("""
  SELECT COUNT(*) AS n
  FROM afiliados_periodo_anterior p
  LEFT JOIN afiliados a ON p.afiliado_id = a.afiliado_id
  WHERE a.afiliado_id IS NULL
""").collect()[0][0]

print(f"✔ Afiliados con cambio de documento : {cambios_documento.count():,}")
print(f"✔ Afiliados retirados (vs periodo ant.): {retirados:,}")
display(cambios_documento.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generar el XML regulatorio
# MAGIC
# MAGIC El proceso actual transforma los datos validados de **CSV a XML** para cargarlos en la plataforma del
# MAGIC ente de control. Aquí generamos el XML del **informe de inconsistencias** directamente desde el
# MAGIC DataFrame — sin persistir tablas intermedias.

# COMMAND ----------

# DBTITLE 1,Construir el XML del informe
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────────────────
# Generación del XML regulatorio directamente desde el DataFrame.
# Cada regla se transforma en un nodo <validacion> XML.
# El XML se construye en memoria sin persistir archivos intermedios.
# ─────────────────────────────────────────────────────────────────────────────
# Un nodo <validacion> por regla, envuelto en <informe_regulatorio>
detalle = (
    informe_inconsistencias
    .select(F.concat(
        F.lit("  <validacion "),
        F.lit("codigo=\""),        F.col("codigo_validacion"), F.lit("\" "),
        F.lit("informe=\""),       F.col("informe"),           F.lit("\" "),
        F.lit("categoria=\""),     F.col("categoria"),         F.lit("\" "),
        F.lit("inconsistencias=\""), F.col("inconsistencias").cast("string"), F.lit("\">"),
        F.col("validacion"),
        F.lit("</validacion>")
    ).alias("nodo"))
    .agg(F.concat_ws("\n", F.collect_list("nodo")).alias("cuerpo"))
    .collect()[0]["cuerpo"]
)

periodo = spark.sql("SELECT date_format(current_date(),'yyyyMM')").collect()[0][0]
xml_regulatorio = (
    f'<?xml version="1.0" encoding="UTF-8"?>\n'
    f'<informe_regulatorio periodo="{periodo}" entidad="COLSUBSIDIO" generado_por="DATABRICKS">\n'
    f'{detalle}\n'
    f'</informe_regulatorio>'
)
print(xml_regulatorio[:1200])

# COMMAND ----------

# DBTITLE 1,Cell 14
# MAGIC %md
# MAGIC ## 6. Escribir resultados de vuelta a SAP HANA
# MAGIC
# MAGIC Los dos productos del proceso — **informe de inconsistencias** y **cambios de documento** — se escriben
# MAGIC de vuelta a SAP HANA usando la **misma Databricks Connection** (`sap_bw_workshop`).
# MAGIC
# MAGIC | Producto | Tabla destino en HANA | Modo |
# MAGIC |----------|----------------------|------|
# MAGIC | Informe de inconsistencias | `WORKSHOP.INFORME_INCONSISTENCIAS` | overwrite |
# MAGIC | Cambios de documento | `WORKSHOP.CAMBIOS_DOCUMENTO` | overwrite |
# MAGIC | XML regulatorio | Archivo para el ente de control | N/A |
# MAGIC
# MAGIC > **Principio:** Databricks **lee → procesa → escribe de vuelta**. No queda ninguna tabla de negocio
# MAGIC > en Databricks. Todo el estado persistente vive en SAP HANA.

# COMMAND ----------

# DBTITLE 1,Write-back a SAP HANA (patrón JDBC)
# ─────────────────────────────────────────────────────────────────────────────
# Escritura de resultados de vuelta a SAP HANA
# Usamos la MISMA conexión registrada para escribir los DataFrames resultantes.
# No se requieren credenciales — la autenticación la maneja Unity Catalog.
# ─────────────────────────────────────────────────────────────────────────────

def escribir_a_hana(df, tabla_destino: str, modo: str = "overwrite"):
    """Escribe un DataFrame a SAP HANA usando la Databricks Connection.
    
    Parámetros:
        df: DataFrame a escribir
        tabla_destino: nombre de la tabla destino en SAP HANA (ej: "INFORME_INCONSISTENCIAS")
        modo: "overwrite" reemplaza la tabla, "append" agrega registros
    """
    (df.write.format("jdbc")
        .option("connectionName", CONNECTION_NAME)
        .option("dbtable", f'"{SAP_SCHEMA}"."{tabla_destino}"')
        .option("batchsize", 10000)    # lotes de 10K filas para rendimiento
        .option("numPartitions", 4)    # 4 escritores paralelos
        .mode(modo)
        .save())
    print(f"✔ Escrito a SAP HANA → {SAP_SCHEMA}.{tabla_destino} ({df.count():,} filas, modo={modo})")

# Escribir los dos productos del ETL de vuelta a HANA
escribir_a_hana(informe_inconsistencias, "INFORME_INCONSISTENCIAS")
escribir_a_hana(cambios_documento,       "CAMBIOS_DOCUMENTO")

# El XML se guardaría como archivo para la plataforma del regulador, p.ej. en un Volume:
# dbutils.fs.put(f"/Volumes/{CATALOGO}/{ESQUEMA}/salida/informe_{periodo}.xml", xml_regulatorio, True)
print("\nℹ El XML regulatorio está listo en la variable `xml_regulatorio` para su entrega.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cierre — de ~40 min a ~10 min
# MAGIC
# MAGIC | Antes (SAP HANA + Python) | Ahora (Databricks) |
# MAGIC |---------------------------|--------------------|
# MAGIC | ~40 min por corrida | Procesamiento distribuido en minutos |
# MAGIC | Sin comparación histórica viable | Join directo contra periodos anteriores |
# MAGIC | Validaciones limitadas por capacidad | Catálogo de reglas ampliable sin costo de rendimiento |
# MAGIC | Datos en SAP HANA | **Datos siguen en SAP HANA** — Databricks solo procesa |
# MAGIC
# MAGIC **Siguiente:** `2 - Pronóstico de Ventas (ETL + ML)` — donde Databricks también entrena y registra un
# MAGIC modelo (el único activo que sí vive en Databricks).
