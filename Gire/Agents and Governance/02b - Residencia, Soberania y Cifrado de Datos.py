# Databricks notebook source
# MAGIC %md
# MAGIC # 02b — 🌎🔐 Residencia, Soberanía y Cifrado de Datos
# MAGIC
# MAGIC **20 min.** Complemento del módulo `02` que cubre dos controles de gobernanza de infraestructura que un comité de riesgo siempre pregunta: **dónde viven los datos** (residencia/soberanía) y **cómo se cifran** (at-rest / in-transit).
# MAGIC
# MAGIC > Módulo **aditivo** — no reemplaza nada del `02`. Cierra dos gaps de la scorecard de gobernanza de proveedores de IA: **residencia de datos (1b)** y **cifrado (1d)**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este módulo — **Secuencial (UI → Code)**
# MAGIC
# MAGIC La residencia y el cifrado se **definen** en la UI (Account Console / workspace settings / Catalog): región del workspace, región del metastore de Unity Catalog, llaves de cifrado. Esa es la fuente de verdad operativa y donde un administrador toma la decisión. **Luego verificamos por código** (SDK + system tables) — porque una afirmación de gobernanza ("nuestros datos están en `us-east-1` y cifrados con CMK") debe ser **auditable y reproducible**, no solo un screenshot. UI **configura**, código **verifica y evidencia**.

# COMMAND ----------

# MAGIC %run ../00_Setup/00_verify_environment

# COMMAND ----------

# Si corres este notebook standalone (sin %run de arriba), descomenta:
# CATALOG = "ardemo_classic_dnubtw_catalog"
# _user = spark.sql("SELECT current_user()").collect()[0][0]
# SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print("Workspace:", spark.conf.get("spark.databricks.workspaceUrl"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1 — Residencia de datos (concern 1b)
# MAGIC
# MAGIC En Databricks la residencia se determina en **tres planos** que conviene verificar por separado:
# MAGIC
# MAGIC | Plano | Qué controla | Dónde se fija |
# MAGIC |---|---|---|
# MAGIC | **Región del workspace** | Dónde corre el compute clásico y el storage del workspace (DBFS root) | Al crear el workspace (Account Console) — inmutable |
# MAGIC | **Región del metastore de UC** | Dónde residen los datos gestionados de Unity Catalog (managed tables/volumes) | Account Console → Catalog → Metastores |
# MAGIC | **Región del serverless** | Dónde corre el compute serverless (SQL, jobs, model serving) | Hereda la región del workspace; verificable y, con *Serverless egress / network*, gobernable |
# MAGIC
# MAGIC **Soberanía:** para requisitos de soberanía (datos que no pueden salir de un país/región), se combina: workspace + metastore en la región requerida, **Storage Locations** de UC apuntando a buckets en esa región, y controles de red (PrivateLink / serverless egress).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 — En la UI (🖱️)
# MAGIC
# MAGIC 1. **Account Console** (`accounts.cloud.databricks.com`) → **Workspaces** → tu workspace → revisa el campo **Region** (p.ej. `us-east-1`). Es la región del compute clásico y del storage del workspace.
# MAGIC 2. **Account Console → Catalog → Metastores**: cada metastore tiene una **región**. Un metastore solo puede asignarse a workspaces de **la misma región** → así UC garantiza co-localización.
# MAGIC 3. **Catalog Explorer → tu catálogo → Details / Storage**: revisa el **Storage Location** (managed location). El bucket/contenedor de ese location define físicamente dónde aterrizan las managed tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 — Verificación por código (auditable)

# COMMAND ----------

# Región efectiva del metastore de Unity Catalog asignado a este workspace
try:
    ms = w.metastores.summary()
    print("Metastore UC:")
    print(f"  nombre:        {ms.name}")
    print(f"  región:        {ms.region}")
    print(f"  cloud:         {ms.cloud}")
    print(f"  storage root:  {ms.default_data_access_config_id and '(configurado)'}")
except Exception as e:
    print("No se pudo leer el metastore (requiere permisos). Nota para el reporte:", e)

# COMMAND ----------

# Storage location (región física) de los catálogos/esquemas accesibles
try:
    rows = spark.sql("""
        SELECT catalog_name, storage_location
        FROM system.information_schema.catalogs
        WHERE storage_location IS NOT NULL
        LIMIT 20
    """)
    print("Storage locations por catálogo (el prefijo s3://.../región revela la residencia física):")
    display(rows)
except Exception as e:
    print("Alternativa: DESCRIBE EXTERNAL LOCATION / DESCRIBE CATALOG. Detalle:", e)

# COMMAND ----------

# External locations registradas (cada una apunta a un bucket en una región concreta)
try:
    print("External Locations (URL revela la región del bucket):")
    for el in w.external_locations.list():
        print(f"  {el.name}: {el.url}")
except Exception as e:
    print("Requiere permisos sobre external locations. Detalle:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC > 📋 **Para el reporte de residencia:** combina (a) región del workspace (UI), (b) `ms.region` del metastore, y (c) el prefijo de región de los `storage_location` / external locations. Si las tres coinciden con la región exigida por el cliente, la residencia está demostrada y es auditable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2 — Cifrado at-rest / in-transit (concern 1d)
# MAGIC
# MAGIC | Capa | Por defecto | Opción reforzada |
# MAGIC |---|---|---|
# MAGIC | **At-rest (storage del workspace + UC)** | Cifrado del proveedor cloud (SSE) gestionado por Databricks | **Customer-Managed Keys (CMK)**: tu propia llave en KMS/Key Vault para DBFS root, managed storage de UC y, en algunos planes, los discos de compute |
# MAGIC | **In-transit** | **TLS 1.2+** en todas las conexiones (entre componentes y hacia el cliente) — siempre activo | PrivateLink para que el tráfico no salga a la internet pública |
# MAGIC | **Managed storage de UC** | Cifrado en reposo en el bucket del managed location | Hereda CMK si está configurada a nivel de metastore/workspace |
# MAGIC
# MAGIC **CMK** te da el control del ciclo de vida de la llave (rotación, revocación → "crypto-shredding"): si revocas la llave, los datos quedan ilegibles aunque estén en el storage.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 — En la UI (🖱️)
# MAGIC
# MAGIC 1. **Account Console → Security and compliance → Encryption keys** (o **Workspaces → tu workspace → Encryption**):
# MAGIC    - **Managed services**: CMK para secretos/notebooks/queries.
# MAGIC    - **Workspace storage**: CMK para DBFS root y, según cloud, discos de cluster.
# MAGIC    - Registras el ARN/ID de tu llave (AWS KMS / Azure Key Vault / GCP KMS) y Databricks la usa para cifrar.
# MAGIC 2. **In-transit:** no hay toggle — **TLS está siempre activo**. Para reforzar, en **Networking** habilita **PrivateLink** (front-end y back-end) para que el tráfico no use la internet pública.
# MAGIC 3. **UC managed storage:** el cifrado del bucket del managed location se ve en la consola del proveedor cloud (SSE-KMS si usas CMK).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 — Verificación / notas por código

# COMMAND ----------

# Confirmar TLS en la URL del workspace (in-transit siempre cifrado)
host = spark.conf.get("spark.databricks.workspaceUrl")
print(f"Endpoint del workspace: https://{host}  → TLS 1.2+ obligatorio (in-transit cifrado por defecto)")

# Estado de CMK del workspace (requiere account admin / API de cuenta)
try:
    ws_info = w.workspace_conf.get_status(keys="enableTokensConfig")  # ejemplo de lectura de config
    print("\nConfig de workspace accesible. Para CMK, consulta la API de cuenta:")
except Exception as e:
    print("\nNota:", e)

print("""
Verificación de CMK (account-level, normalmente fuera del notebook):
  • Account API:  GET /api/2.0/accounts/{account_id}/customer-managed-keys
  • Databricks CLI: databricks account customer-managed-keys list
  • Asocia la llave a 'workspace storage' y/o 'managed services' en la Account Console.
Para el reporte: adjunta el key ARN/ID y el uso (storage vs managed services).
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ **Residencia (1b):** región de workspace + metastore UC + storage locations, verificada por UI **y** código (`metastores.summary()`, `information_schema.catalogs`, external locations).
# MAGIC ✅ **Soberanía:** co-localización de los tres planos + controles de red (PrivateLink / serverless egress).
# MAGIC ✅ **Cifrado (1d):** at-rest (SSE por defecto, **CMK** opcional con tu llave en KMS), in-transit (**TLS 1.2+** siempre), managed storage de UC.
# MAGIC ✅ Patrón **UI → Code**: la UI configura, el código evidencia de forma auditable.
# MAGIC
# MAGIC ## Continuar → `03 - LAB AI Functions (SQL)`
