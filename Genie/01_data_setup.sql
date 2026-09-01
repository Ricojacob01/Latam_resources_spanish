-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 · (OPCIONAL) Datos de demo  ·  se crean en TU esquema
-- MAGIC
-- MAGIC **El camino principal del taller es usar las tablas reales del cliente** (ver `02_genie_code_exploration`).
-- MAGIC Este notebook es un **respaldo genérico**: genera un dataset sintético completo
-- MAGIC (*Cobertura de pipeline por región*) para un *dry run* o cuando el dataset del champion aún no está listo.
-- MAGIC Correrlo en vivo también **genera consumo el Día 1**.
-- MAGIC
-- MAGIC 🔑 **Multi-usuario:** las tablas se crean en **tu propio esquema** (`taller_genie_<usuario>`) dentro del
-- MAGIC catálogo compartido. Cada participante corre este mismo notebook y obtiene **su propia copia** — sin pisarse.
-- MAGIC La configuración la hace `00_config` (abajo con `%run`), así que aquí no hay nada que fijar.
-- MAGIC
-- MAGIC Modelo (estrella): `dim_rep`, `dim_account`, `fact_opportunity`, `fact_region_target`.

-- COMMAND ----------

-- MAGIC %md ### 0. Configuración común (catálogo compartido + tu esquema)
-- MAGIC Corre `00_config` una vez. Si ves el widget **catalog** vacío arriba, llénalo y vuelve a correr.

-- COMMAND ----------

-- MAGIC %run ./00_config

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A partir de aquí, todas las tablas se crean **sin calificar** (nombre simple), por lo que caen
-- MAGIC automáticamente en tu esquema activo (`USE SCHEMA` lo fijó `00_config`).

-- COMMAND ----------

-- MAGIC %md ### 1. `dim_rep` — representantes de ventas

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_rep AS
SELECT
  id AS rep_id,
  concat('Representante ', lpad(cast(id AS string), 3, '0')) AS rep_name,
  element_at(array('México','Colombia','Argentina','Chile','Brasil'), cast(rand(1) * 5 AS int) + 1) AS region,
  element_at(array('Enterprise','Comercial','PyME'), cast(rand(2) * 3 AS int) + 1) AS segment,
  concat('Gerente ', cast(cast(rand(3) * 6 AS int) + 1 AS string)) AS manager_name,
  cast(round(rand(4) * 250000 + 150000, 2) AS decimal(12,2)) AS quarterly_quota
FROM (SELECT explode(sequence(1, 40)) AS id);

COMMENT ON TABLE dim_rep IS 'Representantes de ventas: región, segmento y meta trimestral individual.';
ALTER TABLE dim_rep ALTER COLUMN rep_id COMMENT 'ID único del representante (clave de unión con fact_opportunity.rep_id)';
ALTER TABLE dim_rep ALTER COLUMN region COMMENT 'Región de ventas del representante';
ALTER TABLE dim_rep ALTER COLUMN segment COMMENT 'Segmento de mercado: Enterprise, Comercial o PyME';
ALTER TABLE dim_rep ALTER COLUMN quarterly_quota COMMENT 'Meta (cuota) trimestral asignada al representante, en USD';

-- COMMAND ----------

-- MAGIC %md ### 2. `dim_account` — cuentas / clientes

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_account AS
SELECT
  id AS account_id,
  concat('Cuenta ', lpad(cast(id AS string), 4, '0')) AS account_name,
  element_at(array('México','Colombia','Argentina','Chile','Brasil'), cast(rand(5) * 5 AS int) + 1) AS region,
  element_at(array('Farmacéutica','Retail','Manufactura','Servicios Financieros','Tecnología','Salud'), cast(rand(6) * 6 AS int) + 1) AS industry,
  element_at(array('Enterprise','Comercial','PyME'), cast(rand(7) * 3 AS int) + 1) AS segment
FROM (SELECT explode(sequence(1, 300)) AS id);

COMMENT ON TABLE dim_account IS 'Cuentas/clientes con región, industria y segmento.';
ALTER TABLE dim_account ALTER COLUMN account_id COMMENT 'ID único de la cuenta (clave de unión con fact_opportunity.account_id)';
ALTER TABLE dim_account ALTER COLUMN industry COMMENT 'Industria de la cuenta';

-- COMMAND ----------

-- MAGIC %md ### 3. `fact_opportunity` — oportunidades del pipeline (hecho central)

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_opportunity AS
WITH base AS (
  SELECT
    id AS opp_id,
    cast(rand(10) * 40 AS int) + 1 AS rep_id,
    cast(rand(11) * 300 AS int) + 1 AS account_id,
    element_at(array('Software','Servicios','Hardware','Datos y IA'), cast(rand(12) * 4 AS int) + 1) AS product_line,
    element_at(array('Prospección','Calificación','Propuesta','Negociación','Cerrada Ganada','Cerrada Perdida'), cast(rand(13) * 6 AS int) + 1) AS stage,
    cast(round(rand(14) * 95000 + 5000, 2) AS decimal(12,2)) AS amount,
    date_add(current_date(), -cast(rand(15) * 180 AS int)) AS created_date
  FROM (SELECT explode(sequence(1, 5000)) AS id)
)
SELECT
  b.opp_id,
  b.account_id,
  b.rep_id,
  r.region,
  r.segment,
  b.product_line,
  b.stage,
  b.amount,
  CASE b.stage
    WHEN 'Prospección'    THEN 0.10
    WHEN 'Calificación'   THEN 0.25
    WHEN 'Propuesta'      THEN 0.50
    WHEN 'Negociación'    THEN 0.75
    WHEN 'Cerrada Ganada' THEN 1.00
    ELSE 0.00
  END AS probability,
  b.created_date,
  date_add(b.created_date, cast(rand(16) * 120 + 15 AS int)) AS close_date,
  concat('FY', cast(year(current_date()) AS string), '-Q', cast(quarter(b.created_date) AS string)) AS fiscal_quarter,
  b.stage NOT IN ('Cerrada Ganada', 'Cerrada Perdida') AS is_open,
  b.stage = 'Cerrada Ganada' AS is_won
FROM base b
JOIN dim_rep r ON b.rep_id = r.rep_id;

COMMENT ON TABLE fact_opportunity IS 'Oportunidades del pipeline de ventas. Hecho central. Una fila por oportunidad.';
ALTER TABLE fact_opportunity ALTER COLUMN stage COMMENT 'Etapa: Prospección, Calificación, Propuesta, Negociación, Cerrada Ganada, Cerrada Perdida';
ALTER TABLE fact_opportunity ALTER COLUMN amount COMMENT 'Monto de la oportunidad en USD';
ALTER TABLE fact_opportunity ALTER COLUMN is_open COMMENT 'TRUE si la oportunidad sigue abierta (no Cerrada Ganada ni Cerrada Perdida). Pipeline abierto = SUM(amount) WHERE is_open';
ALTER TABLE fact_opportunity ALTER COLUMN is_won COMMENT 'TRUE si la oportunidad fue Cerrada Ganada. Ingresos ganados = SUM(amount) WHERE is_won';
ALTER TABLE fact_opportunity ALTER COLUMN fiscal_quarter COMMENT 'Trimestre fiscal derivado de created_date, formato FYYYYY-Qn';

-- COMMAND ----------

-- MAGIC %md ### 4. `fact_region_target` — meta (cuota) trimestral por región

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_region_target AS
SELECT
  region,
  fiscal_quarter,
  cast(round(rand(20) * 2000000 + 1500000, 0) AS decimal(14,2)) AS quota_amount
FROM (SELECT explode(array('México','Colombia','Argentina','Chile','Brasil')) AS region)
CROSS JOIN (SELECT explode(array('FY2026-Q1','FY2026-Q2','FY2026-Q3','FY2026-Q4')) AS fiscal_quarter);

COMMENT ON TABLE fact_region_target IS 'Meta (cuota) de ventas por región y trimestre fiscal. Se une a fact_opportunity por region y fiscal_quarter.';
ALTER TABLE fact_region_target ALTER COLUMN quota_amount COMMENT 'Meta de ventas de la región para el trimestre, en USD. Denominador de la cobertura de pipeline.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Validación rápida + métrica estrella
-- MAGIC Confirma los datos y observa la **cobertura de pipeline por región**.

-- COMMAND ----------

SELECT 'dim_rep' AS tabla, count(*) AS filas FROM dim_rep
UNION ALL SELECT 'dim_account', count(*) FROM dim_account
UNION ALL SELECT 'fact_opportunity', count(*) FROM fact_opportunity
UNION ALL SELECT 'fact_region_target', count(*) FROM fact_region_target;

-- COMMAND ----------

-- Cobertura de pipeline por región = pipeline abierto ÷ meta
SELECT
  o.region,
  round(sum(CASE WHEN o.is_open THEN o.amount ELSE 0 END), 0) AS pipeline_abierto,
  round(t.meta, 0) AS meta,
  round(sum(CASE WHEN o.is_open THEN o.amount ELSE 0 END) / t.meta, 2) AS cobertura
FROM fact_opportunity o
JOIN (SELECT region, sum(quota_amount) AS meta FROM fact_region_target GROUP BY region) t
  ON o.region = t.region
GROUP BY o.region, t.meta
ORDER BY cobertura DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ✅ **Listo.** Tienes 4 tablas en tu esquema `taller_genie_<usuario>`.
-- MAGIC Sigue con **`02_genie_code_exploration`** para explorarlas con **Genie Code**.
