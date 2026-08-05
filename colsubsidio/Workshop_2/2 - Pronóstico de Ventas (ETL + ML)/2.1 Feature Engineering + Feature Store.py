# Databricks notebook source
# MAGIC %md
# MAGIC # 2.1 · Ingeniería de Características + Feature Store
# MAGIC ### Pronóstico de ventas — Retail Farma (Colsubsidio)
# MAGIC
# MAGIC Este es el **primer paso del ciclo de vida de Machine Learning**. Antes de entrenar cualquier modelo,
# MAGIC necesitamos transformar los datos crudos (que viven en SAP HANA) en **características** (*features*):
# MAGIC señales numéricas que el modelo puede aprender. Y en lugar de guardarlas en cualquier tabla, las
# MAGIC registramos en el **Databricks Feature Store**, que aporta gobernanza, linaje y reutilización.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-1-v2.png?raw=true" width="1000">
# MAGIC
# MAGIC #### El flujo completo del Módulo 2 (MLOps de extremo a extremo)
# MAGIC
# MAGIC | Notebook | Etapa del ciclo de vida ML | Qué aporta |
# MAGIC |----------|----------------------------|------------|
# MAGIC | **2.1 (este)** | Ingeniería de características + Feature Store | Features gobernadas y reutilizables |
# MAGIC | 2.2 | AutoML | Baseline automático, comparación de algoritmos |
# MAGIC | 2.3 | Entrenamiento + MLflow | Modelo con seguimiento, linaje y firma |
# MAGIC | 2.4 | Modelos en Unity Catalog | Ciclo de vida con alias Challenger/Champion |
# MAGIC | 2.5 | Inferencia batch + write-back | Pronóstico en producción hacia SAP HANA |
# MAGIC
# MAGIC > **Este marco es reutilizable.** Aunque aquí lo aplicamos a un *pronóstico de ventas*, las **mismas 5
# MAGIC > etapas** sirven para cualquier modelo de Colsubsidio: predicción de retiro de afiliados (*churn*),
# MAGIC > detección de mora, propensión a productos, riesgo de crédito, etc. Solo cambian las features y la
# MAGIC > etiqueta; la estructura MLOps es idéntica. Al final de cada notebook encontrarás una sección
# MAGIC > **"Cómo aplicar este marco a otros modelos"**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📘 ¿Qué es el Feature Store y por qué usarlo?
# MAGIC
# MAGIC El **Feature Store** (Feature Engineering in Unity Catalog) es un repositorio centralizado de
# MAGIC características para Machine Learning. Técnicamente, una *feature table* es una tabla Delta con una
# MAGIC **clave primaria declarada** — pero esa declaración desbloquea capacidades que un `saveAsTable` normal
# MAGIC no tiene:
# MAGIC
# MAGIC | Capacidad | `saveAsTable` normal | **Feature Store** |
# MAGIC |-----------|:---:|:---:|
# MAGIC | Guardar features como tabla Delta | ✅ | ✅ |
# MAGIC | **Clave primaria** declarada (para lookups) | ❌ | ✅ |
# MAGIC | **`FeatureLookup`** — join automático en entrenamiento e inferencia | ❌ | ✅ |
# MAGIC | **Empaqueta el modelo con sus features** (misma lógica en train e inferencia) | ❌ | ✅ |
# MAGIC | **Linaje** feature → modelo → datos de origen (en Catalog Explorer) | Parcial | ✅ |
# MAGIC | **Reutilización** entre equipos, modelos y casos de uso | ❌ | ✅ |
# MAGIC | **Online serving** (baja latencia para tiempo real) | ❌ | ✅ |
# MAGIC
# MAGIC #### Los tres problemas que resuelve
# MAGIC
# MAGIC 1. **Skew entrenamiento/servicio (*training-serving skew*).** El error más común y costoso en ML de
# MAGIC    producción: la lógica de features en entrenamiento y en inferencia se implementa dos veces y
# MAGIC    *diverge*. El Feature Store guarda la definición **una sola vez**; entrenamiento e inferencia
# MAGIC    (`create_training_set` y `score_batch`) recuperan **exactamente** las mismas features por su clave.
# MAGIC
# MAGIC 2. **Duplicación de esfuerzo.** Si un equipo ya calculó "ventas promedio de los últimos 28 días por
# MAGIC    familia", otro equipo (p. ej. el modelo de abastecimiento) puede **reutilizar** esa feature en vez
# MAGIC    de recalcularla. Las features se vuelven activos compartidos.
# MAGIC
# MAGIC 3. **Gobernanza y trazabilidad.** Al estar en Unity Catalog, la feature table hereda permisos, y su
# MAGIC    **linaje** permite responder "¿qué modelos usan esta feature?" y "¿de qué tabla de origen viene?" —
# MAGIC    clave para auditoría y análisis de causa raíz cuando un modelo se degrada.
# MAGIC
# MAGIC #### Offline vs. Online
# MAGIC - **Offline store** (lo que usamos hoy): la tabla Delta en UC, para entrenamiento e inferencia **batch**.
# MAGIC - **Online store** (opcional, producción tiempo real): una réplica de baja latencia (Databricks Online
# MAGIC   Tables / Lakebase) para servir features a un endpoint de modelo en milisegundos. La **misma** feature
# MAGIC   table alimenta ambos, sin reescribir lógica.

# COMMAND ----------

# DBTITLE 1,Dependencias
# MAGIC %pip install --quiet databricks-feature-engineering mlflow --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 0 — Parámetros
# MAGIC Derivamos el **esquema propio del usuario** (`ws2_<usuario>`) para que cada participante tenga sus
# MAGIC features aisladas dentro del catálogo compartido. La feature table se llamará `ft_ventas_features`
# MAGIC (prefijo `ft_` = *feature table*, una convención útil para distinguirlas de las tablas de datos).

# COMMAND ----------

# DBTITLE 1,Parámetros — catálogo y esquema del usuario
dbutils.widgets.text("catalogo", "classic_stable_paco_catalog", "Catálogo compartido")

CATALOGO = dbutils.widgets.get("catalogo").strip()
usuario  = spark.sql("SELECT current_user()").collect()[0][0]
SUFIJO   = usuario.split("@")[0].replace(".", "_").replace("-", "_")
ESQUEMA  = f"ws2_{SUFIJO}"
FQN      = f"{CATALOGO}.{ESQUEMA}"

FEATURE_TABLE = f"{FQN}.ft_ventas_features"   # tabla de features en UC

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")
print(f"Origen (stand-in SAP HANA): {FQN}")
print(f"Feature table (UC)        : {FEATURE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1 — Leer datos de origen desde SAP HANA
# MAGIC
# MAGIC Igual que en los módulos ETL: en **producción** se lee desde SAP HANA vía la Databricks Connection
# MAGIC (sin credenciales en el código); en el **laboratorio** leemos las tablas sintéticas equivalentes que
# MAGIC generó `0 - SETUP`. La bandera `LEER_DESDE_HANA` alterna entre ambos modos — así el mismo notebook
# MAGIC funciona en el taller y en producción sin cambios de lógica.

# COMMAND ----------

# DBTITLE 1,Lectura ventas + datos externos
# Lectura directa desde el catálogo Unity Catalog
ventas   = spark.table(f"{FQN}.sap_ventas_historico")
externos = spark.table(f"{FQN}.sap_datos_externos")
print(f"✔ ventas   : {ventas.count():,} filas  (desde {FQN}.sap_ventas_historico)")
print(f"✔ externos : {externos.count():,} filas  (desde {FQN}.sap_datos_externos)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2 — Lógica de ingeniería de características
# MAGIC
# MAGIC **Buena práctica clave:** encapsulamos la creación de features en una **función pura**
# MAGIC (`construir_features`). ¿Por qué una función y no código suelto?
# MAGIC
# MAGIC * **Reutilizable** — la misma función se usa aquí (para poblar el Feature Store) y podría usarse en un
# MAGIC   pipeline de reentrenamiento programado.
# MAGIC * **Testeable** — se puede probar de forma aislada con datos de ejemplo.
# MAGIC * **Legible** — separa el *qué* (las features) del *cómo* (leer/escribir).
# MAGIC
# MAGIC #### Las señales que construimos y por qué
# MAGIC
# MAGIC | Familia de feature | Columnas | Intuición de negocio |
# MAGIC |--------------------|----------|----------------------|
# MAGIC | **Lags** (rezagos) | `lag_1, lag_7, lag_14, lag_28` | Las ventas de hoy se parecen a las de ayer, la semana pasada y hace un mes (autocorrelación) |
# MAGIC | **Medias móviles** | `ma_7, ma_28` | Nivel reciente de demanda, suavizando el ruido diario |
# MAGIC | **Calendario** | `dow, mes, doy, es_finde, es_decembrina` | Estacionalidad: fines de semana y temporada decembrina venden distinto |
# MAGIC | **Macro (externas)** | `ipc_inflacion_anual, trm, tasa_interes, icc` | Contexto económico (Banco de la República, DANE) que afecta el consumo |
# MAGIC
# MAGIC La **clave primaria** será `(producto_familia, fecha)` — identifica de forma única cada fila de features
# MAGIC y es la clave por la que el modelo recuperará las features en entrenamiento e inferencia.
# MAGIC
# MAGIC > ⚠️ **Nota sobre lags y *point-in-time*:** los lags y medias móviles usan **solo información pasada**
# MAGIC > (`rowsBetween(-28, -1)` excluye la fila actual). Esto evita *data leakage* — usar información del
# MAGIC > futuro que no estaría disponible al momento de predecir. Es la versión simple del principio de
# MAGIC > *point-in-time correctness* que el Feature Store formaliza para casos más complejos.

# COMMAND ----------

# DBTITLE 1,Función de featurización
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window

def construir_features(ventas: DataFrame, externos: DataFrame) -> DataFrame:
    """Construye features de pronóstico. PK: (producto_familia, fecha).

    Usa solo información pasada para lags/medias móviles (sin data leakage).
    Reutilizable en entrenamiento y en reentrenamiento programado.
    """
    w = Window.partitionBy("producto_familia").orderBy("fecha")
    return (
        ventas
        .join(externos, on="periodo", how="left")
        .withColumn("lag_1",  F.lag("unidades", 1).over(w))
        .withColumn("lag_7",  F.lag("unidades", 7).over(w))
        .withColumn("lag_14", F.lag("unidades", 14).over(w))
        .withColumn("lag_28", F.lag("unidades", 28).over(w))
        .withColumn("ma_7",  F.avg("unidades").over(w.rowsBetween(-7, -1)))
        .withColumn("ma_28", F.avg("unidades").over(w.rowsBetween(-28, -1)))
        .withColumn("dow",   F.dayofweek("fecha"))
        .withColumn("mes",   F.month("fecha"))
        .withColumn("doy",   F.dayofyear("fecha"))
        .withColumn("es_finde",      F.when(F.dayofweek("fecha").isin(1, 7), 1).otherwise(0))
        .withColumn("es_decembrina", F.when(F.dayofyear("fecha").between(330, 360), 1).otherwise(0))
        .filter(F.col("lag_28").isNotNull() & F.col("ma_28").isNotNull())
        .select(
            "producto_familia", "fecha", "periodo",
            "lag_1", "lag_7", "lag_14", "lag_28", "ma_7", "ma_28",
            "dow", "mes", "doy", "es_finde", "es_decembrina",
            "ipc_inflacion_anual", "trm", "tasa_interes", "icc_confianza_consumidor",
            F.col("unidades").alias("label_unidades"),
        )
    )

features = construir_features(ventas, externos)
print(f"✔ Filas con features completas: {features.count():,}")
display(features.orderBy("producto_familia", "fecha").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 3 — Guardar en el Feature Store (Unity Catalog)
# MAGIC
# MAGIC Usamos el **`FeatureEngineeringClient`** — el SDK oficial del Feature Store. La operación central es
# MAGIC **`create_table`**, que registra la tabla con su clave primaria:
# MAGIC
# MAGIC ```python
# MAGIC fe.create_table(
# MAGIC     name=FEATURE_TABLE,                     # nombre completo en UC: catalogo.esquema.tabla
# MAGIC     primary_keys=["producto_familia","fecha"],  # ← lo que la convierte en feature table
# MAGIC     df=features,                            # datos iniciales
# MAGIC     description="...",                      # documentación (visible en Catalog Explorer)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Hacemos la celda **idempotente**: si la tabla ya existe la actualizamos con `write_table(mode="merge")`
# MAGIC (upsert por clave primaria), y si no, la creamos. Esto permite re-ejecutar el notebook sin errores —
# MAGIC importante en un taller donde los participantes corren las celdas varias veces.

# COMMAND ----------

# DBTITLE 1,Crear/actualizar la Feature Table
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Idempotente: si existe, hacemos merge (upsert por PK); si no, la creamos.
existe = spark.catalog.tableExists(FEATURE_TABLE)
if not existe:
    fe.create_table(
        name=FEATURE_TABLE,
        primary_keys=["producto_familia", "fecha"],
        df=features,
        description="Features de pronóstico de ventas por familia de producto y fecha. "
                    "Derivadas de sap_ventas_historico + sap_datos_externos (origen SAP HANA).",
    )
    print(f"✔ Feature table creada: {FEATURE_TABLE}")
else:
    fe.write_table(name=FEATURE_TABLE, df=features, mode="merge")
    print(f"✔ Feature table actualizada (merge): {FEATURE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 4 — Verificar la tabla de features
# MAGIC
# MAGIC Confirmamos el conteo de filas y familias. En **Catalog Explorer → tu esquema → `ft_ventas_features`**
# MAGIC verás además la pestaña de **Feature** con la clave primaria declarada, la descripción, y (una vez
# MAGIC entrenado el modelo en 2.3) el **linaje** hacia el modelo que la consume.

# COMMAND ----------

# DBTITLE 1,Verificar la tabla de features
display(spark.sql(f"SELECT COUNT(*) AS filas, COUNT(DISTINCT producto_familia) AS familias FROM {FEATURE_TABLE}"))
display(spark.table(FEATURE_TABLE).orderBy("producto_familia", "fecha").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 Ejercicio — Añade una feature de calendario
# MAGIC
# MAGIC Agrega a `construir_features` una columna `inicio_mes` (1 si el día es de los primeros 5 del mes, 0 si no)
# MAGIC — útil porque muchas compras institucionales se concentran a comienzo de mes. Vuelve a ejecutar y
# MAGIC actualiza la feature table (la celda idempotente hará el merge automáticamente).
# MAGIC
# MAGIC <details><summary>💡 Pista</summary>
# MAGIC
# MAGIC ```python
# MAGIC .withColumn("inicio_mes", F.when(F.dayofmonth("fecha") <= 5, 1).otherwise(0))
# MAGIC # recuerda añadirla al .select(...) también
# MAGIC ```
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔁 Cómo aplicar este marco a otros modelos
# MAGIC
# MAGIC El patrón de este notebook es **agnóstico al caso de uso**. Para construir el Feature Store de otro
# MAGIC modelo de Colsubsidio, solo cambian tres cosas:
# MAGIC
# MAGIC | Elemento | Pronóstico de ventas (hoy) | Ejemplo: predicción de retiro de afiliados (*churn*) |
# MAGIC |----------|----------------------------|------------------------------------------------------|
# MAGIC | **Origen** | `sap_ventas_historico`, `sap_datos_externos` | `sap_afiliados`, `sap_aportes`, `sap_personas_cargo` |
# MAGIC | **Clave primaria** | `(producto_familia, fecha)` | `(afiliado_id)` o `(afiliado_id, periodo)` |
# MAGIC | **Features** | lags, medias móviles, calendario, macro | antigüedad, nº servicios usados, mora reciente, cambios de salario |
# MAGIC | **Etiqueta** | `label_unidades` (regresión) | `se_retiro` (clasificación) |
# MAGIC
# MAGIC **Lo que NO cambia** (la estructura reutilizable):
# MAGIC 1. Encapsular la lógica en una función `construir_features`.
# MAGIC 2. Declarar la **clave primaria** y registrar con `fe.create_table`.
# MAGIC 3. Usar solo información pasada (evitar *data leakage*).
# MAGIC 4. Documentar la tabla para gobernanza y reutilización.
# MAGIC
# MAGIC > Una feature table bien diseñada puede alimentar **varios modelos a la vez**. Por ejemplo, features de
# MAGIC > afiliados podrían servir tanto a un modelo de churn como a uno de propensión a productos — sin
# MAGIC > recalcularlas.
# MAGIC
# MAGIC ## Siguiente paso
# MAGIC
# MAGIC Ya tenemos la **feature table** gobernada en Unity Catalog. Continúa con:
# MAGIC * **2.2 AutoML** — genera un modelo baseline automáticamente
# MAGIC * **2.3 Entrenamiento** — entrena usando la feature table con `FeatureLookup` y MLflow
