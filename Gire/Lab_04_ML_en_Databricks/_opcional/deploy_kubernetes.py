# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup del lab
# MAGIC
# MAGIC Catálogo compartido: `workshop_databricks`. Schema personal por usuario: `ws_<usuario>`.
# MAGIC Esta celda valida acceso y crea tu schema si no existe.

# COMMAND ----------

CATALOG = catalog = CATALOGO = "workshop_databricks"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = db = ESQUEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE SCHEMA {CATALOG}.{SCHEMA}")
spark.conf.set("c.catalog", CATALOG)
spark.conf.set("c.schema", SCHEMA)

print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print(f"User:    {_user}")


# MAGIC %md
# MAGIC #Despliegue del Modelo en Kubernetes
# MAGIC Hemos entrenado un modelo en Databricks, lo hemos registrado en MLflow Model Registry, y ahora queremos sacarlo del entorno Databricks para.
# MAGIC
# MAGIC En este notebook vamos a recorrer todo ese camino paso a paso, desde la autenticación hasta el despliegue.
# MAGIC
# MAGIC Aprenderás a:
# MAGIC * Conectarte a tu workspace de Databricks desde tu entorno on-prem
# MAGIC * Descargar un modelo registrado en MLflow
# MAGIC * Ejecutarlo localmente y hacer inferencias
# MAGIC * Empaquetarlo en Docker
# MAGIC * Desplegarlo en Kubernetes con un manifiesto simple

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Prerrequisitos
# MAGIC
# MAGIC Antes de empezar asegúrate de tener instalado lo siguiente:
# MAGIC
# MAGIC * Python 3.8+ y las librerías necesarias: (pip install --upgrade mlflow databricks-cli)
# MAGIC
# MAGIC También necesitas:
# MAGIC * Docker Desktop (para construir y correr contenedores).
# MAGIC * Acceso a tu workspace de Databricks.
# MAGIC * Un Personal Access Token (PAT) o autenticación SSO configurada.
# MAGIC
# MAGIC 📚 Referencias oficiales:
# MAGIC * [Databricks CLI (Unified Auth)](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/)
# MAGIC * [MLflow Models overview](https://mlflow.org/docs/latest/ml/model/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Autenticación con Databricks CLI
# MAGIC
# MAGIC Vale, lo primero es poder hablar con nuestro workspace desde fuera.
# MAGIC El Databricks CLI es la forma más sencilla de autenticarnos desde terminal o scripts locales.
# MAGIC
# MAGIC 🔸 Paso 1 — Login
# MAGIC `databricks auth login \
# MAGIC   --host https://<tu-workspace>.azuredatabricks.net`
# MAGIC
# MAGIC
# MAGIC Esto te pedirá abrir un enlace o pegar tu token.
# MAGIC
# MAGIC 🔸 Paso 2 — Comprobar conexión
# MAGIC `databricks workspace ls /`
# MAGIC
# MAGIC
# MAGIC Si ves la lista de carpetas de tu workspace, ¡ya estás dentro! 🎉
# MAGIC
# MAGIC 📘 Docs: [CLI Authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/authentication)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configurar entorno Python on-prem
# MAGIC
# MAGIC Ahora que tenemos acceso al workspace, vamos a preparar MLflow para apuntar al tracking server de Databricks.
# MAGIC
# MAGIC En una celda:
# MAGIC ```
# MAGIC import mlflow
# MAGIC # Apunta al tracking server de Databricks
# MAGIC mlflow.set_tracking_uri("databricks")
# MAGIC
# MAGIC # (Opcional) Si usas token, puedes definirlo como variable de entorno
# MAGIC # %env DATABRICKS_HOST=https://<tu-workspace>.azuredatabricks.net
# MAGIC # %env DATABRICKS_TOKEN=<tu_token>
# MAGIC ```
# MAGIC
# MAGIC 📘 Docs: [MLflow Tracking URIs](https://mlflow.org/docs/latest/ml/tracking/#where-runs-are-recorded)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Entendiendo cómo se referencia un modelo
# MAGIC
# MAGIC En MLflow podemos apuntar a un modelo de varias formas:  
# MAGIC
# MAGIC | Tipo | Ejemplo | Uso |
# MAGIC | -- | -- | -- |
# MAGIC | Por versión      | models:/catalogo.esquema.modelo/version  | Para reproducibilidad exacta |
# MAGIC | Por alias | models:/catalogo.esquema.modelo@alias              | Ideal para despliegues |
# MAGIC
# MAGIC 📘 Docs: [MLflow Model Registry URIs](https://mlflow.org/docs/latest/ml/model-registry/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Descargar el modelo desde MLflow
# MAGIC
# MAGIC Vale, ahora sí, vamos a descargar el modelo localmente.
# MAGIC ```
# MAGIC import mlflow
# MAGIC
# MAGIC mlflow.set_registry_uri("databricks-uc")  
# MAGIC
# MAGIC # Descarga los artefactos
# MAGIC model_uri = "models:/<CATALOGO>.<ESQUEMA>.<MODELO>/1"  # o "@Champion"
# MAGIC local_dir = "/tmp/mi_modelo"
# MAGIC
# MAGIC mlflow.artifacts.download_artifacts(artifact_uri=model_uri, dst_path=local_dir))
# MAGIC ```
# MAGIC Esto te deja una carpeta con todo: el modelo, el MLmodel, dependencias y metadata.
# MAGIC
# MAGIC 📘 Docs: [download_artifacts](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.artifacts.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cargar el modelo y hacer inferencia local
# MAGIC
# MAGIC Una vez descargado, podemos cargarlo directamente en memoria y probarlo.
# MAGIC
# MAGIC ```
# MAGIC import mlflow
# MAGIC import pandas as pd
# MAGIC
# MAGIC # Carga desde ruta local y predice
# MAGIC import pandas as pd
# MAGIC model = mlflow.pyfunc.load_model(local_dir)
# MAGIC X = pd.DataFrame([{"feature1": 0.3, "feature2": 1.2}])
# MAGIC print(model.predict(X))
# MAGIC
# MAGIC # Ejemplo de predicción
# MAGIC X = pd.DataFrame([
# MAGIC     {"feature1": 0.3, "feature2": 1.2},
# MAGIC     {"feature1": 0.8, "feature2": 3.1},
# MAGIC ])
# MAGIC
# MAGIC preds = model.predict(X)
# MAGIC ```
# MAGIC
# MAGIC 📘 Docs: [mlflow.pyfunc.load_model](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.pyfunc.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Empaquetar el modelo como contenedor Docker
# MAGIC
# MAGIC Como ya tienes el modelo descargado a disco, puedes construir el contenedor directamente desde esa carpeta, para convertirlo en una API lista para servir inferencias.
# MAGIC
# MAGIC ```
# MAGIC mlflow models build-docker \
# MAGIC   -m "local_dir" \ #apunta al path local donde está el modelo
# MAGIC   -n "<tu_repo>/<tu_imagen>:v1" \ #nombre y etiqueta de la imagen Docker que se generará
# MAGIC   --enable-mlserver
# MAGIC ```
# MAGIC Esto construye una imagen con:
# MAGIC
# MAGIC * Python runtime
# MAGIC
# MAGIC * El modelo
# MAGIC
# MAGIC * MLServer (servidor de inferencia REST)
# MAGIC
# MAGIC Puedes verla en tu lista de imágenes locales:
# MAGIC
# MAGIC * docker images | grep <tu_imagen>
# MAGIC
# MAGIC
# MAGIC 📘 Docs: [Deploy MLflow Models to Docker](https://mlflow.org/docs/latest/ml/model/#build-a-docker-image)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Probar el contenedor localmente
# MAGIC 1) Ejecuta el contenedor
# MAGIC
# MAGIC `docker run --rm -p 8080:8080 "<tu_repo>/<tu_imagen>:v1"`
# MAGIC
# MAGIC 2) En otra terminal, envía una predicción:
# MAGIC
# MAGIC ```
# MAGIC curl -X POST http://localhost:8080/invocations \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{"inputs": [{"feature1": 0.3, "feature2": 1.2}]}'
# MAGIC ```
# MAGIC Si obtienes un JSON con predicciones, ya tienes tu modelo sirviendo en tu máquina local 🚀
# MAGIC
# MAGIC Docs: [Serving models locally](https://mlflow.org/docs/latest/ml/model/#local-deployment)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Desplegar el modelo en Kubernetes
# MAGIC
# MAGIC Ahora vamos a llevar ese mismo contenedor a Kubernetes con un manifiesto básico.
# MAGIC
# MAGIC Crea un archivo k8s-mlflow-model.yaml con este contenido:
# MAGIC ```
# MAGIC apiVersion: apps/v1
# MAGIC kind: Deployment
# MAGIC metadata:
# MAGIC   name: mlflow-model
# MAGIC spec:
# MAGIC   replicas: 2
# MAGIC   selector:
# MAGIC     matchLabels:
# MAGIC       app: mlflow-model
# MAGIC   template:
# MAGIC     metadata:
# MAGIC       labels:
# MAGIC         app: mlflow-model
# MAGIC     spec:
# MAGIC       containers:
# MAGIC       - name: mlflow-model
# MAGIC         image: <tu_repo>/<tu_imagen>:v1
# MAGIC         ports:
# MAGIC         - containerPort: 8080
# MAGIC ---
# MAGIC apiVersion: v1
# MAGIC kind: Service
# MAGIC metadata:
# MAGIC   name: mlflow-model-svc
# MAGIC spec:
# MAGIC   selector:
# MAGIC     app: mlflow-model
# MAGIC   ports:
# MAGIC   - port: 80
# MAGIC     targetPort: 8080
# MAGIC ```
# MAGIC
# MAGIC Despliega con:
# MAGIC ```
# MAGIC kubectl apply -f k8s-mlflow-model.yaml
# MAGIC kubectl get pods
# MAGIC ```
# MAGIC
# MAGIC Y para probarlo:
# MAGIC ```
# MAGIC kubectl port-forward svc/mlflow-model-svc 8080:80
# MAGIC curl -X POST http://localhost:8080/invocations \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{"inputs": [{"feature1": 0.3, "feature2": 1.2}]}'
# MAGIC ```
# MAGIC
# MAGIC 📘 Docs: [Deploy MLflow Model to Kubernetes](https://mlflow.org/docs/latest/ml/model/#deployment)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¡Felicidades! Ya tendríamos funcionando nuestro modelo en local
# MAGIC
# MAGIC Ahora vamos a continuar aprendiendo sobre las [AI functions]($./03_AI_functions)
# MAGIC
