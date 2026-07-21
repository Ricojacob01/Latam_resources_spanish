-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Garantizando la gobernanza y seguridad para nuestro lakehouse
-- MAGIC
-- MAGIC La gobernanza y seguridad de datos es difícil cuando se trata de una plataforma de datos completa. El comando SQL GRANT en tablas no es suficiente y la seguridad debe aplicarse a múltiples activos de datos (tableros, modelos, archivos, etc.).
-- MAGIC
-- MAGIC Para reducir riesgos y fomentar la innovación, el equipo de Gobernanza de Datos necesita:
-- MAGIC
-- MAGIC - Unificar todos los activos de datos (Tablas, Archivos, Modelos de ML, Características, Tableros, Consultas)
-- MAGIC - Incorporar datos con múltiples equipos
-- MAGIC - Compartir y monetizar activos con organizaciones externas
-- MAGIC
-- MAGIC <style>
-- MAGIC .box{
-- MAGIC   box-shadow: 20px -20px #CCC; height:300px; box-shadow:  0 0 10px  rgba(0,0,0,0.3); padding: 5px 10px 0px 10px;}
-- MAGIC .badge {
-- MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
-- MAGIC .badge_b { 
-- MAGIC   height: 35px}
-- MAGIC </style>
-- MAGIC <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
-- MAGIC <div style="padding: 20px; font-family: 'DM Sans'; color: #1b5162">
-- MAGIC   <div style="width:200px; float: left; text-align: center">
-- MAGIC     <div class="box" style="">
-- MAGIC       <div style="font-size: 26px;">
-- MAGIC         <strong>Equipo A</strong>
-- MAGIC       </div>
-- MAGIC       <div style="font-size: 13px">
-- MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/alice.png" style="" width="60px"> <br/>
-- MAGIC         Analistas de Datos<br/>
-- MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/marc.png" style="" width="60px"> <br/>
-- MAGIC         Científicos de Datos<br/>
-- MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/john.png" style="" width="60px"> <br/>
-- MAGIC         Ingenieros de Datos
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC     <div class="box" style="height: 80px; margin: 20px 0px 50px 0px">
-- MAGIC       <div style="font-size: 26px;">
-- MAGIC         <strong>Equipo B</strong>
-- MAGIC       </div>
-- MAGIC       <div style="font-size: 13px">...</div>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC   <div style="float: left; width: 400px; padding: 0px 20px 0px 20px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">Permisos en consultas, tableros</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">Permisos en tablas, columnas, filas</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">Permisos en características, modelos de ML, endpoints, notebooks…</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">Permisos en archivos, trabajos</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC   </div>
-- MAGIC   
-- MAGIC   <div class="box" style="width:550px; float: left">
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/emily.png" style="float: left; margin-right: 10px;" width="80px"> 
-- MAGIC     <div style="float: left; font-size: 26px; margin-top: 0px; line-height: 17px;"><strong>Andrea</strong> <br />Gobernanza y Seguridad</div>
-- MAGIC     <div style="font-size: 17px; clear: left; padding-top: 10px">
-- MAGIC       <ul style="line-height: 2px;">
-- MAGIC         <li>Catálogo central - todos los activos de datos</li>
-- MAGIC         <li>Exploración y descubrimiento de datos para desbloquear nuevos casos de uso</li>
-- MAGIC         <li>Permisos entre equipos</li>
-- MAGIC         <li>Reducir riesgos con registros de auditoría</li>
-- MAGIC         <li>Medir el impacto con linaje</li>
-- MAGIC       </ul>
-- MAGIC       + Monetizar y compartir datos con organizaciones externas (Delta Sharing)
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <!-- Recopilar datos de uso (vista). Elimínelo para deshabilitar la recopilación o desactive el rastreador durante la instalación. Vea el README para más detalles.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&org_id=1444828305810485&notebook=%2F02-Data-governance%2F02.1-UC-data-governance-security-churn&demo_name=lakehouse-retail-c360&event=VIEW&path=%2F_dbdemos%2Flakehouse%2Flakehouse-retail-c360%2F02-Data-governance%2F02.1-UC-data-governance-security-churn&version=1&user_hash=f7ea13a45c991650d8df810431c3e0e2b12887e9ed7e206ee8fb6209bdb2ae82">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Utilice un cluster Serverless Environment 2 para ejecutar este notebook
-- MAGIC Para ejecutar esta demostración, simplemente selecciona el cluster `Serverless` en el menú desplegable.
-- MAGIC Comprueba que la versión del cluster serverless es la número 2 <br />
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ![](./files/images/version2-serverless.png)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Gobernanza de Datos Escalable con Unity Catalog  
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/cross_demo_assets/Lakehouse_Demo_Team_architecture_2.png?raw=true" style="float: right" width="500px">
-- MAGIC
-- MAGIC
-- MAGIC Gestionar el acceso seguro y escalable a los datos es fundamental. Con **Unity Catalog**, el **Lakehouse** permite una gobernanza sencilla mientras asegura que los equipos puedan colaborar eficientemente.  
-- MAGIC
-- MAGIC ### El Desafío  
-- MAGIC Nuestros datos, almacenados como **Tablas Delta**, deben estar protegidos pero accesibles para diferentes equipos:  
-- MAGIC - **Ingenieros de Datos** gestionan y actualizan los conjuntos de datos principales.  
-- MAGIC - **Científicos de Datos** leen tablas finales y refinan conjuntos de características.  
-- MAGIC - **Analistas** exploran y transforman datos dentro de esquemas gobernados.  
-- MAGIC - **El acceso se enmascara/anónima dinámicamente** según los roles de usuario.  
-- MAGIC
-- MAGIC ### La Solución: Unity Catalog  
-- MAGIC Al centralizar el control de acceso, **Unity Catalog** permite:  
-- MAGIC ✅ **ACLs** de grano fino  
-- MAGIC ✅ **Registros de auditoría** para cumplimiento  
-- MAGIC ✅ **Linaje de datos** para transparencia  
-- MAGIC ✅ **Exploración y descubrimiento sencillos**  
-- MAGIC ✅ **Compartición de datos sin fricciones** entre equipos y organizaciones (**Delta Sharing**)  
-- MAGIC
-- MAGIC Con **Unity Catalog**, los equipos pueden gestionar con confianza la **gobernanza, seguridad y colaboración** entre espacios de trabajo. 🚀

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Explorando nuestra base de datos
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="800px"/> 
-- MAGIC
-- MAGIC Vamos a revisar los datos creados.
-- MAGIC
-- MAGIC Unity Catalog funciona con 3 capas:
-- MAGIC
-- MAGIC * CATÁLOGO
-- MAGIC * SCHEMA (o BASE DE DATOS)
-- MAGIC * TABLA
-- MAGIC
-- MAGIC Todo Unity Catalog está disponible con SQL (`CREATE CATALOG IF NOT EXISTS mi_catalogo` ...)
-- MAGIC
-- MAGIC Para acceder a una tabla, puedes especificar la ruta completa: `SELECT * FROM <CATÁLOGO>.<ESQUEMA>.<TABLA>`

-- COMMAND ----------

USE CATALOG ardemo_classic_dnubtw_catalog;
USE SCHEMA sdp_workshop_rico_martinez_bronze;

-- COMMAND ----------

SELECT CURRENT_CATALOG();

-- COMMAND ----------

SELECT CURRENT_SCHEMA();

-- COMMAND ----------

-- DBTITLE 1,Create Raw/Bronze customer data from IBM Telco public dataset and sanitize column name
-- MAGIC %python
-- MAGIC # Default for quickstart
-- MAGIC bronze_table_name = "bronze_customers"
-- MAGIC
-- MAGIC import requests
-- MAGIC import pandas as pd
-- MAGIC import re
-- MAGIC
-- MAGIC from io import StringIO
-- MAGIC #Dataset under apache license: https://github.com/IBM/telco-customer-churn-on-icp4d/blob/master/LICENSE
-- MAGIC csv = requests.get("https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv").text
-- MAGIC df = pd.read_csv(StringIO(csv), sep=",")
-- MAGIC
-- MAGIC def cleanup_column(pdf):
-- MAGIC   # Clean up column names
-- MAGIC   pdf.columns = [re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower().replace("__", "_") for name in pdf.columns]
-- MAGIC   pdf.columns = [re.sub(r'[\(\)]', '', name).lower() for name in pdf.columns]
-- MAGIC   pdf.columns = [re.sub(r'[ -]', '_', name).lower() for name in pdf.columns]
-- MAGIC   return pdf.rename(columns = {'streaming_t_v': 'streaming_tv', 'customer_i_d': 'customer_id'})
-- MAGIC
-- MAGIC df = cleanup_column(df)
-- MAGIC print(f"creating `{bronze_table_name}` raw table")
-- MAGIC spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table_name)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Revisemos las tablas que creamos bajo nuestro schema
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-data-explorer.gif" style="float: right" width="800px"/> 
-- MAGIC
-- MAGIC Unity Catalog proporciona un completo Explorador de Datos al que puedes acceder en el menú de la izquierda.
-- MAGIC
-- MAGIC Encontrarás todas tus tablas y podrás usarlo para acceder y administrar tus tablas.
-- MAGIC
-- MAGIC Podrán crear tablas adicionales en este esquema.
-- MAGIC
-- MAGIC ### Descubribimiento
-- MAGIC
-- MAGIC Además, Unity Catalog también facilita la exploración y el descubrimiento de datos.
-- MAGIC
-- MAGIC Cualquier persona con acceso a las tablas podrá buscarlas y analizar su uso principal. <br>
-- MAGIC Puedes usar el menú de Búsqueda (⌘ + P) para navegar por tus activos de datos (tablas, notebooks, consultas...)

-- COMMAND ----------

-- DBTITLE 1,As you can see, our tables are available under our catalog.
SHOW TABLES

-- COMMAND ----------

-- DBTITLE 1,Granting access to Analysts & Data Engineers:
-- Vamos a otorgar a nuestros ANALISTAS el permiso de SELECT:
-- Nota: asegúrate de haber creado previamente los grupos analysts y dataengineers desde la consola de la cuenta.
GRANT SELECT ON TABLE bronze_customers TO `analysts`;

-- Otorgaremos un permiso adicional de MODIFY a nuestro Ingeniero de Datos
GRANT SELECT, MODIFY ON SCHEMA bronze TO `dataengineers`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Enmascaramiento de datos PII, filtrado a nivel de fila y columna
-- MAGIC
-- MAGIC En las siguientes celdas demostraremos cómo manejar datos sensibles mediante el enmascaramiento de columnas y filas.
-- MAGIC
-- MAGIC Primero, vamos a crear una nueva tabla llamada "mlops_churn_protected" con algunas columnas adicionales

-- COMMAND ----------

CREATE OR REPLACE TABLE churn_protected AS
SELECT
  *,
  "exemplo@email.com" AS email,
  "Jorge"             AS firstname,
  "Vidal"             AS lastname,
  "Calle 123"         AS address,
  CASE
    WHEN ROW_NUMBER() OVER (ORDER BY customer_id) <= (COUNT(*) OVER () / 2) THEN 'Chile' 
    ELSE 'Brazil'
  END                 AS country
FROM bronze_customers;

-- COMMAND ----------

SELECT * FROM churn_protected;

-- COMMAND ----------

SELECT DISTINCT country
FROM churn_protected;

-- COMMAND ----------

-- El grupo de retail_admin va a tener acceso a todos los datos, el resto de usuarios va a ver la información enmascarada 
CREATE OR REPLACE FUNCTION simple_mask(column_value STRING)
   RETURN IF(is_account_group_member('retail_admin'), column_value, "****");
   
-- Enmascara toda la información PII
ALTER TABLE churn_protected ALTER COLUMN email SET MASK simple_mask;
ALTER TABLE churn_protected ALTER COLUMN firstname SET MASK simple_mask;
ALTER TABLE churn_protected ALTER COLUMN lastname SET MASK simple_mask;
ALTER TABLE churn_protected ALTER COLUMN address SET MASK simple_mask;

-- Aplica un filtro de filas para la columna país 
CREATE OR REPLACE FUNCTION country_filter(country STRING) 
RETURN (
  is_account_group_member('retail_admin') or  -- el grupo retail_admin puede ver todas las regiones de la tabla
  country like "Chile"                 -- los usuarios que no son retail_admins solo pueden ver la región Chile
);                 

ALTER TABLE churn_protected SET ROW FILTER country_filter ON (country);

SELECT * FROM churn_protected

-- COMMAND ----------

SELECT DISTINCT country
FROM churn_protected;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Avanzando con la gobernanza y seguridad de datos
-- MAGIC
-- MAGIC Al reunir todos tus activos de datos, Unity Catalog te permite construir una gobernanza completa y sencilla para ayudarte a escalar tus equipos.
-- MAGIC
-- MAGIC Unity Catalog puede ser utilizado desde simples GRANT hasta la construcción de una organización datamesh completa.
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/lineage/lineage-table.gif" style="float: right; margin-left: 10px"/>
-- MAGIC
-- MAGIC ### Linaje
-- MAGIC
-- MAGIC UC captura automáticamente las dependencias de las tablas y te permite rastrear cómo se usa tu información, incluso a nivel de fila: `dbdemos.install('uc-03-data-lineage')`
-- MAGIC
-- MAGIC Esto te permite analizar el impacto aguas abajo o monitorear información sensible en toda la organización (GDPR).
-- MAGIC
-- MAGIC ### Registro de auditoría
-- MAGIC
-- MAGIC UC captura todos los eventos. ¿Necesitas saber quién accede a qué datos? Consulta tu registro de auditoría: `dbdemos.install('uc-04-audit-log')`
-- MAGIC
-- MAGIC Esto te permite analizar el impacto aguas abajo o monitorear información sensible en toda la organización (GDPR).
-- MAGIC
-- MAGIC ### Actualización a UC
-- MAGIC
-- MAGIC ¿Ya usas Databricks sin UC? Actualizar tus tablas para beneficiarte de Unity Catalog es simple: `dbdemos.install('uc-05-upgrade')`
-- MAGIC
-- MAGIC ### Compartir datos con organizaciones externas
-- MAGIC
-- MAGIC Compartir tus datos fuera de tus usuarios de Databricks es simple con Delta Sharing, y no requiere que los consumidores de datos usen Databricks: `dbdemos.install('delta-sharing-airlines')`