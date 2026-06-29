# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 _resources/00-setup — Configuración compartida del Workshop
# MAGIC
# MAGIC **Este notebook NO se ejecuta solo.** Cada módulo lo invoca al inicio con:
# MAGIC
# MAGIC ```python
# MAGIC %run ../_resources/00-setup
# MAGIC ```
# MAGIC
# MAGIC Deja listo, de forma **idempotente**, todo lo que el workshop necesita:
# MAGIC - Catálogo `ardemo_classic_dnubtw_catalog` y **schema personal** `ws_<usuario>` (cada asistente el suyo).
# MAGIC - Un **volume** para los documentos de la base de conocimiento.
# MAGIC - Las **tablas semilla** del caso de uso *Agente de Servicios al Afiliado Comfama*:
# MAGIC   `programas`, `afiliados`, `beneficios_afiliado` y `kb_documentos` (con Change Data Feed para Vector Search).
# MAGIC - Las **constantes compartidas** (nombres de endpoints, índice, proyecto Lakebase) que usan los demás módulos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catálogo y schema personal

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import date

# Catálogo del workspace (ya existe). Schema personal por asistente para evitar colisiones.
CATALOG = "ardemo_classic_dnubtw_catalog"
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username = current_user.split("@")[0].replace(".", "_").replace("-", "_")
SCHEMA = f"ws_{username}"

spark.sql(f"USE CATALOG {CATALOG}")  # el catálogo del workspace ya existe; solo lo usamos
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Volume para los documentos fuente de la base de conocimiento
VOLUME = "kb_docs"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

print(f"✅ Usuario:  {current_user}")
print(f"✅ Catálogo: {CATALOG}")
print(f"✅ Schema:   {SCHEMA}")
print(f"✅ Volume:   /Volumes/{CATALOG}/{SCHEMA}/{VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Constantes compartidas
# MAGIC Nombres únicos por asistente para Vector Search, Model Serving, AI Gateway, Lakebase y la App.

# COMMAND ----------

# Vector Search
VS_ENDPOINT = "comfama_vs_endpoint"          # endpoint compartido (uno por workspace)
VS_INDEX    = f"{CATALOG}.{SCHEMA}.kb_index"  # índice por asistente
EMBEDDING_MODEL = "databricks-gte-large-en"   # FM de embeddings managed (ajusta si tu workspace usa otro)

# Agente / Model Serving / AI Gateway
LLM_ENDPOINT      = "databricks-meta-llama-3-3-70b-instruct"  # FM que razona el agente
AGENT_MODEL_NAME  = f"{CATALOG}.{SCHEMA}.agente_afiliados"     # modelo del agente en UC
AGENT_ENDPOINT    = f"agente_afiliados_{username}"            # serving endpoint del agente
EXPERIMENT_PATH   = f"/Users/{current_user}/comfama_agente"    # experimento MLflow POR ASISTENTE

# Lakebase (provisioned tier)
LAKEBASE_PROJECT   = "comfama-afiliados"        # INSTANCIA compartida (una por workspace; ideal: la crea el instructor)
LAKEBASE_DB        = f"comfama_{username}"        # base de datos POR ASISTENTE → aísla reservas/cupos
LAKEBASE_BRANCH    = f"ws-{username}".replace("_", "-")  # (solo tier Autoscaling) branch personal

# Databricks App
APP_NAME = f"agente-afiliados-{username}".replace("_", "-")

# Compartido (una vez por workspace) vs por-asistente — útil tenerlo claro en un workshop multiusuario
print("Constantes cargadas:")
for k in ["VS_ENDPOINT","VS_INDEX","EMBEDDING_MODEL","LLM_ENDPOINT","AGENT_MODEL_NAME",
          "AGENT_ENDPOINT","EXPERIMENT_PATH","LAKEBASE_PROJECT","LAKEBASE_DB","APP_NAME"]:
    print(f"  {k:18}= {eval(k)}")
print("\n  (compartidos por workspace: VS_ENDPOINT, LAKEBASE_PROJECT, LLM_ENDPOINT — el resto es por-asistente)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tablas semilla — caso de uso *Agente de Servicios al Afiliado*
# MAGIC Datos de referencia (reproducibles). Se re-escriben en cada `%run` para que cualquiera pueda *resetear* el workshop.
# MAGIC
# MAGIC > Nota: la capa **operacional mutable** (cupos en vivo, reservas, conversaciones) vivirá en **Lakebase** (módulo 03).
# MAGIC > Estas tablas Delta son la **semilla** y la fuente analítica que luego se sincroniza a Lakebase.

# COMMAND ----------

# --- Programas (catálogo de servicios de Comfama) ---
programas_data = [
    (1,  "Vacaciones Recreativas Niños",      "recreación", "Programa vacacional con actividades lúdicas y deportivas para niños de 6 a 12 años.", "Comfama Rionegro",      "Rionegro", 40, 12,  "presencial", 35000.0),
    (2,  "Curso de Natación",                 "salud",      "Clases de natación por niveles para todas las edades, con instructores certificados.", "Comfama Aburrá Sur",  "Itagüí",   25, 5,   "presencial", 48000.0),
    (3,  "Diplomado Excel Avanzado",          "educación",  "Diplomado de 40 horas en Excel avanzado, tablas dinámicas y automatización.",          "Comfama Centro",        "Medellín", 30, 0,   "híbrido",    60000.0),
    (4,  "Taller de Emprendimiento",          "educación",  "Taller práctico para estructurar un modelo de negocio y plan financiero.",             "Comfama Pedregal",      "Medellín", 20, 8,   "presencial", 0.0),
    (5,  "Jornada de Salud Preventiva",       "salud",      "Tamizaje, vacunación y orientación en hábitos saludables para afiliados.",             "Comfama Salud",         "Medellín", 50, 30,  "presencial", 0.0),
    (6,  "Orientación Subsidio de Vivienda",  "subsidios",  "Asesoría personalizada para postular al subsidio de vivienda Comfama.",                 "Comfama Sede Principal","Medellín", 100,75,  "virtual",    0.0),
    (7,  "Liga de Fútbol Infantil",           "recreación", "Liga recreativa de fútbol para niños y niñas, fines de semana.",                       "Comfama Guayabal",      "Medellín", 60, 14,  "presencial", 25000.0),
    (8,  "Curso de Inglés A1",                "educación",  "Curso de inglés nivel principiante (A1), 60 horas.",                                   "Comfama Envigado",      "Envigado", 35, 3,   "híbrido",    72000.0),
    (9,  "Yoga y Bienestar",                  "salud",      "Sesiones de yoga y mindfulness para el manejo del estrés.",                            "Comfama Bello",         "Bello",    30, 18,  "presencial", 30000.0),
    (10, "Cine Club Familiar",                "recreación", "Funciones de cine para la familia con entrada preferencial para afiliados.",            "Comfama Centro",        "Medellín", 80, 52,  "presencial", 8000.0),
    (11, "Escuela de Música",                 "educación",  "Formación musical en guitarra, piano y técnica vocal por niveles.",                    "Comfama La Playa",      "Medellín", 22, 2,   "presencial", 90000.0),
    (12, "Campamento Juvenil",                "recreación", "Campamento de 3 días con actividades al aire libre para jóvenes de 13 a 17 años.",      "Comfama Guatapé",       "Guatapé",  45, 20,  "presencial", 120000.0),
]
programas_schema = ("programa_id INT, nombre STRING, categoria STRING, descripcion STRING, "
                    "sede STRING, ciudad STRING, cupos_totales INT, cupos_disponibles INT, "
                    "modalidad STRING, costo_afiliado DOUBLE")
(spark.createDataFrame(programas_data, programas_schema)
      .write.mode("overwrite").option("overwriteSchema","true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.programas"))

# --- Afiliados ---
afiliados_data = [
    (1001, "María Restrepo",     "43111222", "A", "maria.restrepo@example.com",   "Medellín", date(2019, 3, 12)),
    (1002, "Juan Carlos Gómez",  "71222333", "B", "jc.gomez@example.com",         "Itagüí",   date(2021, 7, 1)),
    (1003, "Laura Vélez",        "32333444", "A", "laura.velez@example.com",      "Envigado", date(2018,11,23)),
    (1004, "Andrés Mejía",       "98444555", "C", "andres.mejia@example.com",     "Bello",    date(2022, 1,15)),
    (1005, "Catalina Ruiz",      "43555666", "B", "catalina.ruiz@example.com",    "Medellín", date(2020, 5, 9)),
    (1006, "Santiago Arango",    "71666777", "A", "santiago.arango@example.com",  "Rionegro", date(2017, 9,30)),
    (1007, "Daniela Ospina",     "32777888", "C", "daniela.ospina@example.com",   "Medellín", date(2023, 2,18)),
    (1008, "Felipe Cardona",     "98888999", "B", "felipe.cardona@example.com",   "Guatapé",  date(2021,12, 5)),
]
afiliados_schema = ("afiliado_id INT, nombre STRING, documento STRING, categoria STRING, "
                    "email STRING, ciudad STRING, fecha_afiliacion DATE")
(spark.createDataFrame(afiliados_data, afiliados_schema)
      .write.mode("overwrite").option("overwriteSchema","true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.afiliados"))

# --- Beneficios/inscripciones históricas del afiliado ---
beneficios_data = [
    (1001, 3,  "completado", date(2025, 2,10)),
    (1001, 5,  "inscrito",   date(2026, 6, 1)),
    (1002, 2,  "inscrito",   date(2026, 6,15)),
    (1003, 8,  "completado", date(2025, 9, 5)),
    (1004, 7,  "cancelado",  date(2026, 3,20)),
    (1005, 10, "inscrito",   date(2026, 6,22)),
    (1006, 1,  "inscrito",   date(2026, 6,25)),
]
beneficios_schema = "afiliado_id INT, programa_id INT, estado STRING, fecha DATE"
(spark.createDataFrame(beneficios_data, beneficios_schema)
      .write.mode("overwrite").option("overwriteSchema","true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.beneficios_afiliado"))

print("✅ Tablas semilla escritas: programas, afiliados, beneficios_afiliado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Base de conocimiento (`kb_documentos`)
# MAGIC Documentos que el agente usará vía **RAG** (Vector Search). Se habilita **Change Data Feed** porque el índice
# MAGIC Delta-Sync lo requiere.

# COMMAND ----------

kb_data = [
    (1, "Categorías de afiliación (A, B, C)", "afiliación",
     "Comfama clasifica a sus afiliados en tres categorías según el salario. Categoría A: ingresos hasta 2 SMMLV, "
     "acceso a los mayores subsidios y tarifas preferenciales. Categoría B: entre 2 y 4 SMMLV, tarifas intermedias. "
     "Categoría C: más de 4 SMMLV, acceso a todos los servicios a tarifa plena. La categoría determina el costo "
     "de programas, el monto de la cuota monetaria y la prioridad en subsidios.",
     "https://www.comfama.com/afiliacion/categorias"),
    (2, "Cómo inscribirse a un programa", "procesos",
     "Para inscribirse a un programa el afiliado debe estar activo, seleccionar el programa según disponibilidad de "
     "cupos, y confirmar la reserva. El cupo se descuenta al confirmar. Si el programa tiene costo, se paga en línea "
     "o en sede. La inscripción puede cancelarse hasta 48 horas antes liberando el cupo.",
     "https://www.comfama.com/servicios/inscripciones"),
    (3, "Subsidio de vivienda", "subsidios",
     "El subsidio de vivienda de Comfama apoya a afiliados de categorías A y B en la compra de vivienda de interés "
     "social. Requisitos: afiliación mínima de 12 meses, no haber recibido el subsidio antes y postularse en las "
     "fechas de convocatoria. El programa 'Orientación Subsidio de Vivienda' brinda asesoría para la postulación.",
     "https://www.comfama.com/subsidios/vivienda"),
    (4, "Cuota monetaria", "subsidios",
     "La cuota monetaria es un aporte mensual por cada persona a cargo del afiliado (hijos menores de edad, padres, "
     "cónyuge sin ingresos). El monto depende de la categoría: mayor para categoría A. Se paga junto con la nómina "
     "del empleador o por consignación directa.",
     "https://www.comfama.com/subsidios/cuota-monetaria"),
    (5, "Programas de recreación", "recreación",
     "Comfama ofrece recreación para toda la familia: vacaciones recreativas para niños, ligas deportivas, cine club, "
     "campamentos juveniles y parques. Las tarifas dependen de la categoría de afiliación. Los cupos son limitados y "
     "se asignan por orden de inscripción.",
     "https://www.comfama.com/recreacion"),
    (6, "Educación y formación", "educación",
     "El portafolio educativo incluye cursos de idiomas, diplomados técnicos, talleres de emprendimiento y escuela de "
     "música. Varios cursos son híbridos. Algunos programas como el Taller de Emprendimiento no tienen costo para el "
     "afiliado. Se entrega certificado al completar.",
     "https://www.comfama.com/educacion"),
    (7, "Servicios de salud", "salud",
     "Comfama Salud ofrece jornadas de salud preventiva, cursos de natación, yoga y bienestar. Las jornadas "
     "preventivas (tamizaje y vacunación) son gratuitas para afiliados. Las actividades deportivas tienen tarifa "
     "preferencial según categoría.",
     "https://www.comfama.com/salud"),
    (8, "Política de cancelación y reembolsos", "procesos",
     "Las inscripciones a programas con costo pueden cancelarse hasta 48 horas antes del inicio para reembolso total. "
     "Cancelaciones posteriores no son reembolsables salvo causa de fuerza mayor documentada. Al cancelar se libera "
     "el cupo para otros afiliados.",
     "https://www.comfama.com/servicios/cancelaciones"),
    (9, "Sedes y cobertura", "general",
     "Comfama tiene presencia en el Valle de Aburrá y el Oriente antioqueño: Medellín (Centro, Guayabal, Pedregal, "
     "La Playa), Itagüí (Aburrá Sur), Envigado, Bello, Rionegro y Guatapé. Cada sede ofrece un subconjunto del "
     "portafolio según su infraestructura.",
     "https://www.comfama.com/sedes"),
    (10, "Preguntas frecuentes (FAQ)", "general",
     "¿Cómo sé mi categoría? Consulta tu perfil de afiliado. ¿Puedo inscribir a mi familia? Sí, a las personas a "
     "cargo registradas. ¿Qué pasa si un programa está lleno? Puedes elegir otro programa o sede; el agente te "
     "muestra alternativas con cupo disponible. ¿Los cursos dan certificado? Sí, al completarlos.",
     "https://www.comfama.com/ayuda/faq"),
]
kb_schema = "doc_id INT, titulo STRING, categoria STRING, contenido STRING, url STRING"
(spark.createDataFrame(kb_data, kb_schema)
      .write.mode("overwrite").option("overwriteSchema","true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.kb_documentos"))

# Change Data Feed es requerido por el índice Delta-Sync de Vector Search
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.kb_documentos "
          f"SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

print(f"✅ kb_documentos escrita ({len(kb_data)} documentos) con Change Data Feed habilitado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup completo
# MAGIC Las variables (`CATALOG`, `SCHEMA`, `VS_ENDPOINT`, `VS_INDEX`, `LAKEBASE_*`, etc.) y las tablas semilla quedan
# MAGIC disponibles para el resto del módulo que hizo `%run` de este notebook.

# COMMAND ----------

print("Setup listo. Tablas en", f"{CATALOG}.{SCHEMA}", ":")
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))

