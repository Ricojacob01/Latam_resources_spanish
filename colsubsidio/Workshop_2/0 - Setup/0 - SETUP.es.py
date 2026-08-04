# Databricks notebook source
# MAGIC %md
# MAGIC # 0 · Setup — Colsubsidio · SAP HANA ↔ Databricks
# MAGIC
# MAGIC Este taller demuestra cómo usar **Databricks como motor de procesamiento (ETL + ML)** sobre datos que
# MAGIC viven en **SAP HANA**, sin convertir a Databricks en un repositorio de datos.
# MAGIC
# MAGIC ### Principio del taller
# MAGIC
# MAGIC > **Databricks NO almacena las tablas de los casos de uso.** El procesamiento es **transitorio**
# MAGIC > (DataFrames y vistas temporales) y los resultados se **escriben de vuelta a SAP HANA** vía JDBC.
# MAGIC >
# MAGIC > Lo único que se persiste en el catálogo de Databricks es:
# MAGIC > 1. **Los datos sintéticos de origen** que este notebook genera para *simular SAP HANA* (porque en el
# MAGIC >    laboratorio no hay un HANA real conectado), y
# MAGIC > 2. **El modelo de ML registrado** en el Unity Catalog Model Registry (Módulo 2).
# MAGIC
# MAGIC ### Módulos
# MAGIC
# MAGIC | # | Módulo | Tipo | Tiempo |
# MAGIC |---|--------|------|--------|
# MAGIC | 0 | **Setup** (este notebook) | Generación de datos | ~5 min |
# MAGIC | 1 | **Validación Regulatoria** | ETL | ~45 min |
# MAGIC | 2 | **Pronóstico de Ventas** | ETL + ML | ~45 min |
# MAGIC | — | *SAP HANA Synthetic Orders Demo* | Referencia (patrón JDBC) | — |
# MAGIC
# MAGIC ### Catálogo y esquemas
# MAGIC
# MAGIC - **Catálogo compartido** — todos los participantes usan el mismo catálogo.
# MAGIC - **Esquema por usuario** — cada participante trabaja en su propio esquema `ws2_<tu_usuario>`
# MAGIC   (derivado de `current_user()`), para que no haya colisiones de datos.

# COMMAND ----------

# DBTITLE 1,Widgets — catálogo compartido y escala de datos
dbutils.widgets.text("catalogo", "classic_stable_paco_catalog", "Catálogo compartido")
dbutils.widgets.dropdown("escala", "demo", ["demo", "full"], "Escala de datos")

CATALOGO = dbutils.widgets.get("catalogo").strip()
ESCALA   = dbutils.widgets.get("escala").strip()

# ── Esquema propio por usuario (mismo patrón que Workshop_1) ──────────────────
usuario = spark.sql("SELECT current_user()").collect()[0][0]
SUFIJO  = usuario.split("@")[0].replace(".", "_").replace("-", "_")
ESQUEMA = f"ws2_{SUFIJO}"
FQN     = f"{CATALOGO}.{ESQUEMA}"

# ── Escala: 'demo' es rápida; 'full' refleja los volúmenes reales (>1M filas) ──
if ESCALA == "full":
    N_EMPRESAS, N_AFILIADOS, N_PERSONAS, N_APORTES = 250_000, 1_200_000, 250_000, 1_100_000
else:
    N_EMPRESAS, N_AFILIADOS, N_PERSONAS, N_APORTES = 40_000, 200_000, 150_000, 200_000

print(f"Usuario  : {usuario}")
print(f"Catálogo : {CATALOGO}")
print(f"Esquema  : {ESQUEMA}")
print(f"Escala   : {ESCALA}  →  empresas={N_EMPRESAS:,} afiliados={N_AFILIADOS:,} "
      f"personas={N_PERSONAS:,} aportes={N_APORTES:,}")

# COMMAND ----------

# DBTITLE 1,Crear el esquema del usuario
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQN}")
spark.sql(f"USE SCHEMA {ESQUEMA}")
print(f"✔ Esquema listo: {FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Módulo 1 — Bases regulatorias (simulan SAP HANA)
# MAGIC
# MAGIC El área de **Afiliaciones** entrega mensualmente **4 bases** que Planeación valida antes de reportar al
# MAGIC ente de control. Aquí las generamos con **inconsistencias inyectadas a propósito** para que el Módulo 1
# MAGIC las detecte:
# MAGIC
# MAGIC | Base | Tabla | Descripción |
# MAGIC |------|-------|-------------|
# MAGIC | Empresas / Aportantes | `sap_empresas` | Empresas afiliadas y su estado de aportes |
# MAGIC | Afiliados | `sap_afiliados` | Trabajadores afiliados a la caja |
# MAGIC | Personas a Cargo | `sap_personas_cargo` | Beneficiarios (hijos, padres, cónyuge) |
# MAGIC | Aportes | `sap_aportes` | Aportes mensuales por empresa y periodo |
# MAGIC
# MAGIC > Estas tablas representan lo que en producción vive en **SAP HANA**. En el laboratorio las materializamos
# MAGIC > en el catálogo únicamente porque no hay un HANA real conectado.

# COMMAND ----------

# DBTITLE 1,Base 1 — Empresas / Aportantes
spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.sap_empresas AS
WITH base AS (
  SELECT id, (pmod(hash(id, 1), 1000000) / 1000000.0) AS r_email, (pmod(hash(id, 2), 1000000) / 1000000.0) AS r_len, (pmod(hash(id, 3), 1000000) / 1000000.0) AS r_aporte,
         (pmod(hash(id, 4), 1000000) / 1000000.0) AS r_muni, (pmod(hash(id, 5), 1000000) / 1000000.0) AS r_estado
  FROM range({N_EMPRESAS})
)
SELECT
  -- NIT: normalmente 9 dígitos. Inyectamos longitud incorrecta (validación: longitud vs tipo)
  CASE WHEN r_len < 0.02 THEN LPAD(CAST(id AS STRING), 5, '0')
       ELSE LPAD(CAST(800000000 + id AS STRING), 9, '0') END          AS nit,
  'NIT'                                                                 AS tipo_identificacion,
  CONCAT('Empresa ', LPAD(CAST(id AS STRING), 6, '0'))                  AS razon_social,
  -- Email: inyectamos formatos inválidos (validación: formato correo)
  CASE WHEN r_email < 0.03 THEN CONCAT('contacto', CAST(id AS STRING))               -- sin @dominio
       WHEN r_email < 0.05 THEN CONCAT('contacto', CAST(id AS STRING), '@dominio')   -- sin .com
       ELSE CONCAT('contacto', CAST(id AS STRING), '@empresa', CAST(id % 50 AS STRING), '.com') END AS email,
  -- Municipio donde opera: inyectamos códigos fuera de Cundinamarca(25)/Bogotá(11)
  CASE WHEN r_muni < 0.08 THEN element_at(array('05001','76001','08001','13001','54001'), CAST(r_muni * 5 + 1 AS INT))
       ELSE element_at(array('11001','25001','25269','25290','25473','25754'), CAST(r_muni * 6 + 1 AS INT)) END AS municipio_codigo,
  -- Estado del aportante
  CASE WHEN r_estado < 0.10 THEN 'MORA'
       WHEN r_estado < 0.18 THEN 'RETIRADA'
       ELSE 'ACTIVA' END                                               AS estado,
  -- Aporte mensual: inyectamos aportes inferiores a 100.000 (validación) y <= 0
  CASE WHEN r_aporte < 0.02 THEN ROUND((pmod(hash(id, 9), 1000000) / 1000000.0) * 90000, 0)         -- < 100.000
       WHEN r_aporte < 0.03 THEN 0.0                                    -- <= 0
       ELSE ROUND((pmod(hash(id, 10), 1000000) / 1000000.0) * 4000000 + 150000, 0) END             AS valor_aporte_mensual,
  date_sub(current_date(), CAST((pmod(hash(id, 11), 1000000) / 1000000.0) * 3650 AS INT))           AS fecha_afiliacion,
  'SAP_HANA'                                                            AS source_system
FROM base
""")
print(f"✔ sap_empresas: {spark.table(f'{FQN}.sap_empresas').count():,} filas")

# COMMAND ----------

# DBTITLE 1,Base 2 — Afiliados
spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.sap_afiliados AS
WITH base AS (
  SELECT id,
         (pmod(hash(id, 21), 1000000) / 1000000.0) AS r_doc, (pmod(hash(id, 22), 1000000) / 1000000.0) AS r_len, (pmod(hash(id, 23), 1000000) / 1000000.0) AS r_sal,
         (pmod(hash(id, 24), 1000000) / 1000000.0) AS r_tipo, (pmod(hash(id, 25), 1000000) / 1000000.0) AS r_cat, (pmod(hash(id, 26), 1000000) / 1000000.0) AS r_orf,
         (pmod(hash(id, 27), 1000000) / 1000000.0) AS r_muni, (pmod(hash(id, 28), 1000000) / 1000000.0) AS r_email
  FROM range({N_AFILIADOS})
),
principal AS (
  SELECT
    CAST(id + 1000000 AS BIGINT)                                        AS afiliado_id,
    -- Tipo y número de documento; CC debería tener 8-10 dígitos
    CASE WHEN r_doc < 0.85 THEN 'CC' WHEN r_doc < 0.93 THEN 'CE' ELSE 'TI' END AS tipo_documento,
    -- Longitud incorrecta vs tipo (validación: longitud identificación vs tipo)
    CASE WHEN r_len < 0.03 THEN LPAD(CAST(id AS STRING), 4, '0')
         ELSE LPAD(CAST(10000000 + id AS STRING), 10, '0') END          AS numero_documento,
    CONCAT('Afiliado ', LPAD(CAST(id AS STRING), 7, '0'))               AS nombre,
    CASE WHEN r_email < 0.03 THEN CONCAT('afil', CAST(id AS STRING))
         ELSE CONCAT('afil', CAST(id AS STRING), '@correo.com') END     AS email,
    -- Salario: inyectamos > 100 millones y <= 0 (validaciones)
    CASE WHEN r_sal < 0.005 THEN ROUND((pmod(hash(id, 31), 1000000) / 1000000.0) * 50000000 + 100000001, 0)  -- > 100M
         WHEN r_sal < 0.010 THEN 0.0                                             -- <= 0
         ELSE ROUND((pmod(hash(id, 32), 1000000) / 1000000.0) * 8000000 + 1300000, 0) END           AS salario,
    -- tipo_afiliado 13 = servicio doméstico → debe ser categoría C
    CASE WHEN r_tipo < 0.06 THEN 13 ELSE element_at(array(1,2,3,5,8), CAST(r_tipo * 5 + 1 AS INT)) END AS tipo_afiliado,
    -- Categoría A/B/C; inyectamos tipo 13 con categoría != C (validación)
    CASE WHEN r_tipo < 0.06 AND r_cat < 0.5 THEN element_at(array('A','B'), CAST(r_cat * 2 + 1 AS INT))
         WHEN r_tipo < 0.06 THEN 'C'
         ELSE element_at(array('A','B','C'), CAST(r_cat * 3 + 1 AS INT)) END AS categoria,
    -- Empresa (FK a sap_empresas); inyectamos NIT huérfano (validación cruzada)
    CASE WHEN r_orf < 0.02 THEN LPAD(CAST(999000000 + id AS STRING), 9, '0')
         ELSE LPAD(CAST(800000000 + CAST((pmod(hash(id, 41), 1000000) / 1000000.0) * {N_EMPRESAS} AS INT) AS STRING), 9, '0') END AS empresa_nit,
    -- Municipio donde labora; fuera de Cundinamarca/Bogotá
    CASE WHEN r_muni < 0.08 THEN element_at(array('05001','76001','08001','13001','54001'), CAST(r_muni * 5 + 1 AS INT))
         ELSE element_at(array('11001','25001','25269','25290','25473'), CAST(r_muni * 5 + 1 AS INT)) END AS municipio_labora,
    date_sub(current_date(), CAST((pmod(hash(id, 51), 1000000) / 1000000.0) * 20000 + 6570 AS INT)) AS fecha_nacimiento,
    'ACTIVO'                                                            AS estado,
    'SAP_HANA'                                                          AS source_system
  FROM base
)
SELECT * FROM principal
UNION ALL
-- Inyectamos duplicados por (tipo_documento, numero_documento) (validación: duplicados)
SELECT afiliado_id + 500000000, tipo_documento, numero_documento, nombre, email, salario,
       tipo_afiliado, categoria, empresa_nit, municipio_labora, fecha_nacimiento, estado, source_system
FROM principal
WHERE afiliado_id % 4000 = 0
""")
print(f"✔ sap_afiliados: {spark.table(f'{FQN}.sap_afiliados').count():,} filas")

# COMMAND ----------

# DBTITLE 1,Base 3 — Personas a Cargo (beneficiarios)
spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.sap_personas_cargo AS
WITH base AS (
  SELECT id,
         (pmod(hash(id, 61), 1000000) / 1000000.0) AS r_par, (pmod(hash(id, 62), 1000000) / 1000000.0) AS r_edad, (pmod(hash(id, 63), 1000000) / 1000000.0) AS r_cuota,
         (pmod(hash(id, 64), 1000000) / 1000000.0) AS r_disc, (pmod(hash(id, 65), 1000000) / 1000000.0) AS r_doc, (pmod(hash(id, 66), 1000000) / 1000000.0) AS r_orf,
         (pmod(hash(id, 67), 1000000) / 1000000.0) AS r_cat
  FROM range({N_PERSONAS})
)
SELECT
  CAST(id + 2000000 AS BIGINT)                                          AS persona_id,
  -- FK al afiliado; inyectamos huérfanos (validación cruzada)
  CASE WHEN r_orf < 0.02 THEN CAST(888000000 + id AS BIGINT)
       ELSE CAST(1000000 + CAST((pmod(hash(id, 71), 1000000) / 1000000.0) * {N_AFILIADOS} AS INT) AS BIGINT) END AS afiliado_id,
  CASE WHEN r_par < 0.55 THEN 'HIJO'
       WHEN r_par < 0.75 THEN 'PADRE'
       WHEN r_par < 0.90 THEN 'MADRE'
       ELSE 'CONYUGE' END                                              AS parentesco,
  -- Edad simulada mediante fecha_nacimiento; inyectamos >100 años y menores con CC
  CASE
    WHEN r_edad < 0.01 THEN date_sub(current_date(), CAST(101 * 365.25 + (pmod(hash(id, 81), 1000000) / 1000000.0) * 3000 AS INT)) -- > 100 años
    WHEN r_par < 0.55 AND r_edad < 0.30 THEN date_sub(current_date(), CAST((19 + (pmod(hash(id, 82), 1000000) / 1000000.0) * 8) * 365.25 AS INT)) -- hijo > 18
    WHEN r_par < 0.55 THEN date_sub(current_date(), CAST((pmod(hash(id, 83), 1000000) / 1000000.0) * 17 * 365.25 AS INT))          -- hijo menor
    ELSE date_sub(current_date(), CAST((40 + (pmod(hash(id, 84), 1000000) / 1000000.0) * 45) * 365.25 AS INT))
  END                                                                   AS fecha_nacimiento,
  -- Tipo de documento; menores deberían ser TI/RC. Inyectamos menores con CC (validación)
  CASE WHEN r_doc < 0.05 THEN 'CC' ELSE element_at(array('TI','RC','CC','CE'), CAST(r_doc * 4 + 1 AS INT)) END AS tipo_documento,
  LPAD(CAST(20000000 + id AS STRING), 10, '0')                          AS numero_documento,
  CASE WHEN r_disc < 0.06 THEN 'SI' ELSE 'NO' END                       AS discapacidad,
  -- Recibe cuota monetaria; inyectamos hijos>18 con cuota (validación)
  CASE WHEN r_cuota < 0.40 THEN 'SI' ELSE 'NO' END                      AS recibe_cuota_monetaria,
  element_at(array('A','B','C'), CAST(r_cat * 3 + 1 AS INT))            AS categoria,
  'SAP_HANA'                                                            AS source_system
FROM base
""")
print(f"✔ sap_personas_cargo: {spark.table(f'{FQN}.sap_personas_cargo').count():,} filas")

# COMMAND ----------

# DBTITLE 1,Base 4 — Aportes mensuales
spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.sap_aportes AS
WITH base AS (
  SELECT id, (pmod(hash(id, 91), 1000000) / 1000000.0) AS r_val, (pmod(hash(id, 92), 1000000) / 1000000.0) AS r_estado, (pmod(hash(id, 93), 1000000) / 1000000.0) AS r_orf, (pmod(hash(id, 94), 1000000) / 1000000.0) AS r_per
  FROM range({N_APORTES})
)
SELECT
  CAST(id + 3000000 AS BIGINT)                                          AS aporte_id,
  -- NIT empresa (FK); inyectamos huérfanos y empresas en mora
  CASE WHEN r_orf < 0.02 THEN LPAD(CAST(777000000 + id AS STRING), 9, '0')
       ELSE LPAD(CAST(800000000 + CAST((pmod(hash(id, 95), 1000000) / 1000000.0) * {N_EMPRESAS} AS INT) AS STRING), 9, '0') END AS nit_empresa,
  -- Periodo YYYYMM en los últimos 12 meses; inyectamos periodo futuro (validación cuotas/periodos)
  CASE WHEN r_per < 0.01 THEN date_format(add_months(current_date(), 2), 'yyyyMM')
       ELSE date_format(add_months(current_date(), -CAST((pmod(hash(id, 96), 1000000) / 1000000.0) * 12 AS INT)), 'yyyyMM') END AS periodo,
  -- Valor del aporte; inyectamos < 100.000 y <= 0
  CASE WHEN r_val < 0.02 THEN ROUND((pmod(hash(id, 97), 1000000) / 1000000.0) * 90000, 0)
       WHEN r_val < 0.03 THEN 0.0
       ELSE ROUND((pmod(hash(id, 98), 1000000) / 1000000.0) * 3000000 + 120000, 0) END             AS valor,
  CASE WHEN r_estado < 0.12 THEN 'MORA'
       WHEN r_estado < 0.20 THEN 'VOLUNTARIO'
       ELSE 'PAGADO' END                                               AS estado,
  date_sub(current_date(), CAST((pmod(hash(id, 99), 1000000) / 1000000.0) * 365 AS INT))           AS fecha_pago,
  'SAP_HANA'                                                            AS source_system
FROM base
""")
print(f"✔ sap_aportes: {spark.table(f'{FQN}.sap_aportes').count():,} filas")

# COMMAND ----------

# DBTITLE 1,Snapshot del periodo anterior — para comparación histórica
# MAGIC %md
# MAGIC Para la **comparación histórica** (detectar cambios de documento de identidad entre periodos),
# MAGIC generamos una foto del **periodo anterior** de afiliados. A un subconjunto le cambiamos el
# MAGIC `numero_documento` (simula corrección/cambio de identidad) y quitamos algunos (retiros).

# COMMAND ----------

# DBTITLE 1,Base — Afiliados periodo anterior
spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.sap_afiliados_periodo_anterior AS
SELECT
  afiliado_id,
  tipo_documento,
  -- A ~1.5% le cambiamos el número de documento (validación: cambio de documento entre periodos)
  CASE WHEN afiliado_id % 67 = 0 THEN LPAD(CAST(90000000 + afiliado_id % 1000000 AS STRING), 10, '0')
       ELSE numero_documento END                                       AS numero_documento,
  nombre,
  salario,
  categoria,
  empresa_nit,
  'SAP_HANA'                                                           AS source_system
FROM {FQN}.sap_afiliados
-- Simulamos retiros: quitamos ~3% que estaban el periodo anterior
WHERE afiliado_id % 33 <> 0
""")
print(f"✔ sap_afiliados_periodo_anterior: {spark.table(f'{FQN}.sap_afiliados_periodo_anterior').count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Módulo 2 — Datos para pronóstico (simulan SAP HANA)
# MAGIC
# MAGIC | Tabla | Descripción |
# MAGIC |-------|-------------|
# MAGIC | `sap_ventas_historico` | 4 años de ventas diarias por familia de producto (Bodega LAE Retail) |
# MAGIC | `sap_datos_externos` | Indicadores macro mensuales (Banco de la República / DANE) |
# MAGIC
# MAGIC La serie de ventas incluye **tendencia + estacionalidad semanal y anual + efecto de indicadores
# MAGIC macro + ruido**, para que el modelo tenga señal real que aprender.

# COMMAND ----------

# DBTITLE 1,Datos externos — indicadores macro (Banco de la República / DANE)
spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.sap_datos_externos AS
WITH meses AS (
  SELECT date_format(add_months(trunc(current_date(),'MM'), -seq), 'yyyyMM') AS periodo, seq
  FROM (SELECT explode(sequence(0, 47)) AS seq)
)
SELECT
  periodo,
  ROUND(5.5 + sin(seq / 6.0) * 2.5 + (pmod(hash(seq, 1), 1000000) / 1000000.0) * 0.8, 2)            AS ipc_inflacion_anual,   -- DANE
  ROUND(4200 - seq * 12 + sin(seq / 4.0) * 180 + (pmod(hash(seq, 2), 1000000) / 1000000.0) * 90, 1) AS trm,                    -- BanRep
  ROUND(11.0 - seq * 0.05 + sin(seq / 8.0) * 1.5 + (pmod(hash(seq, 3), 1000000) / 1000000.0) * 0.5, 2) AS tasa_interes,        -- BanRep
  ROUND(sin(seq / 5.0) * 20 + (pmod(hash(seq, 4), 1000000) / 1000000.0) * 8 - 4, 1)                AS icc_confianza_consumidor -- Fedesarrollo/DANE
FROM meses
""")
print(f"✔ sap_datos_externos: {spark.table(f'{FQN}.sap_datos_externos').count():,} filas")

# COMMAND ----------

# DBTITLE 1,Ventas históricas — 4 años diarios por familia de producto
spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.sap_ventas_historico AS
WITH dias AS (
  SELECT explode(sequence(date_sub(current_date(), 1460), date_sub(current_date(), 1), interval 1 day)) AS fecha
),
familias AS (
  SELECT explode(array('Tecnología','Hogar','Salud','Moda','Alimentos','Ferretería')) AS producto_familia
),
cruz AS (
  SELECT
    d.fecha,
    f.producto_familia,
    datediff(d.fecha, date_sub(current_date(), 1460))                   AS t,         -- índice de tiempo
    dayofweek(d.fecha)                                                  AS dow,
    dayofyear(d.fecha)                                                  AS doy,
    -- nivel base por familia
    element_at(map('Tecnología',900,'Hogar',700,'Salud',1100,'Moda',600,'Alimentos',1500,'Ferretería',500),
               f.producto_familia)                                      AS base_nivel
  FROM dias d CROSS JOIN familias f
)
SELECT
  fecha,
  date_format(fecha, 'yyyyMM')                                          AS periodo,
  producto_familia,
  CAST(GREATEST(0, ROUND(
      base_nivel
      * (1 + t * 0.00025)                                              -- tendencia
      * (1 + 0.18 * sin(2 * 3.14159 * doy / 365.0))                     -- estacionalidad anual
      * (CASE WHEN dow IN (1,7) THEN 1.25 ELSE 1.0 END)                 -- fin de semana
      * (CASE WHEN doy BETWEEN 330 AND 360 THEN 1.4 ELSE 1.0 END)       -- temporada decembrina
      + ((pmod(hash(t, producto_familia), 1000000) / 1000000.0) - 0.5) * base_nivel * 0.12  -- ruido
  )) AS INT)                                                            AS unidades,
  'SAP_HANA'                                                            AS source_system
FROM cruz
""")
# Añadir venta monetaria derivada
spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.sap_ventas_historico AS
SELECT *,
       ROUND(unidades * element_at(
         map('Tecnología',85000.0,'Hogar',45000.0,'Salud',30000.0,'Moda',60000.0,'Alimentos',12000.0,'Ferretería',38000.0),
         producto_familia), 0)                                         AS venta_neta
FROM {FQN}.sap_ventas_historico
""")
print(f"✔ sap_ventas_historico: {spark.table(f'{FQN}.sap_ventas_historico').count():,} filas")

# COMMAND ----------

# DBTITLE 1,Resumen — tablas generadas
resumen = spark.sql(f"""
SELECT 'sap_empresas'                    AS tabla, COUNT(*) AS filas FROM {FQN}.sap_empresas
UNION ALL SELECT 'sap_afiliados',                  COUNT(*) FROM {FQN}.sap_afiliados
UNION ALL SELECT 'sap_personas_cargo',             COUNT(*) FROM {FQN}.sap_personas_cargo
UNION ALL SELECT 'sap_aportes',                    COUNT(*) FROM {FQN}.sap_aportes
UNION ALL SELECT 'sap_afiliados_periodo_anterior', COUNT(*) FROM {FQN}.sap_afiliados_periodo_anterior
UNION ALL SELECT 'sap_ventas_historico',           COUNT(*) FROM {FQN}.sap_ventas_historico
UNION ALL SELECT 'sap_datos_externos',             COUNT(*) FROM {FQN}.sap_datos_externos
ORDER BY tabla
""")
display(resumen)

# COMMAND ----------

# DBTITLE 1,Guardar parámetros para los módulos siguientes
# Los módulos 1 y 2 leen estos valores desde task values / widgets.
print("Copia estos valores para usarlos en los Módulos 1 y 2:\n")
print(f"  CATALOGO = {CATALOGO}")
print(f"  ESQUEMA  = {ESQUEMA}")
print(f"  FQN      = {FQN}\n")
print("✔ Setup completado. Continúa con:")
print("   1 - Validación Regulatoria (ETL)")
print("   2 - Pronóstico de Ventas (ETL + ML)")

