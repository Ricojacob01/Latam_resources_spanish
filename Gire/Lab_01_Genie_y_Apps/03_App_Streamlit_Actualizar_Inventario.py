# Databricks notebook source
# MAGIC %md
# MAGIC # Taller: App Streamlit para actualizar inventario + chatbot (Parte 3)
# MAGIC
# MAGIC Objetivos:
# MAGIC - Construir una App (Streamlit) dentro de Databricks para actualizar en tiempo real la tabla `inventario_insumos_oficina`.
# MAGIC - Agregar un chatbot básico para consultas sobre la tabla.
# MAGIC
# MAGIC Referencia: databricks-apps-cookbook
# MAGIC - `https://github.com/databricks-solutions/databricks-apps-cookbook/`
# MAGIC
# MAGIC Nota: Esta app puede ejecutarse como Databricks App o desde un notebook (modo demo). Ajusta según tu entorno.
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup del lab
# MAGIC
# MAGIC Catálogo compartido: `workshop_databricks`. Schema personal por usuario: `ws_<usuario>`.
# MAGIC Esta celda valida acceso y crea tu schema si no existe.

# COMMAND ----------

CATALOG = catalog = CATALOGO = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = db = ESQUEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")

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

import streamlit as st
import pandas as pd
from databricks import sql
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient ####


# -----------------------------
# CONFIGURACIÓN DEL CATÁLOGO/TABLA
# -----------------------------
CATALOGO = CATALOG  # del setup cell (workshop_databricks)
ESQUEMA = SCHEMA  # schema personal (definido en celda Setup)
TABLA = "inventario_insumos_oficina"  # tu tabla

TABLE_FULL_NAME = f"{CATALOGO}.{ESQUEMA}.{TABLA}"

# -----------------------------
# CONFIGURACIÓN DATBRICKS SQL
# -----------------------------
cfg = Config()  # Debe tener DATABRICKS_HOST y DATABRICKS_TOKEN configurados en el entorno

st.set_page_config(page_title="Inventario Oficina", layout="wide")
st.title("Inventario de Insumos de Oficina")

# --- Conexión SQL ---
@st.cache_resource(ttl="1h")  # Cachea la conexión
def get_connection(http_path: str):
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )

# --- Leer tabla ---
def read_table(table_name: str, conn):
    with conn.cursor() as cursor:
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

# --- Actualizar stock ---
def update_stock(table_name: str, item_id: str, nuevo_stock: int, conn):
    with conn.cursor() as cursor:
        query = f"""
        UPDATE {table_name}
        SET stock_actual = {nuevo_stock}
        WHERE item_id = '{item_id}'
        """
        cursor.execute(query)

# -----------------------------
# INPUT HTTP PATH
# -----------------------------
http_path_input = st.text_input(
    "Enter your Databricks HTTP Path:", placeholder="/sql/1.0/warehouses/xxxxxx"
)

if http_path_input:
    # Crear conexión
    conn = get_connection(http_path_input)
    df = read_table(TABLE_FULL_NAME, conn)
    
    st.subheader("Vista previa")
    st.dataframe(df.head(50))
    
    # --- Actualizar stock ---
    st.subheader("Actualizar stock")
    item_id = st.text_input("Item ID (ej.: ITM0001)", key="update_id")
    nuevo_stock = st.number_input("Nuevo stock", min_value=0, step=1, key="update_stock")
    
    if st.button("Actualizar"):
        if item_id:
            update_stock(TABLE_FULL_NAME, item_id, nuevo_stock, conn)
            st.success(f"Stock actualizado para {item_id} → {nuevo_stock}")
            df = read_table(TABLE_FULL_NAME, conn)
            st.dataframe(df.head(50))
        else:
            st.warning("Ingresa un Item ID válido.")
    
    # --- Filtros ---
    st.subheader("Filtrar por categoría / subcategoría")
    categorias = ["(todas)"] + df["categoria"].dropna().unique().tolist()
    cat_sel = st.selectbox("Categoría", options=categorias, key="cat")
    
    sub_list = []
    if cat_sel != "(todas)":
        sub_list = ["(todas)"] + df[df["categoria"] == cat_sel]["subcategoria"].dropna().unique().tolist()
    else:
        sub_list = ["(todas)"]
    sub_sel = st.selectbox("Subcategoría", options=sub_list, key="subcat")
    
    # Filtrar DataFrame
    df_filtered = df.copy()
    if cat_sel != "(todas)":
        df_filtered = df_filtered[df_filtered["categoria"] == cat_sel]
    if sub_sel != "(todas)":
        df_filtered = df_filtered[df_filtered["subcategoria"] == sub_sel]
    
    st.write("Resultados filtrados:")
    st.dataframe(df_filtered.head(200))

    # --- Gráfica de los top productos filtrados ---
    st.subheader("Top productos filtrados por stock")

    if not df_filtered.empty:
        # Ordenar por stock_actual descendente
        top_products = df_filtered.sort_values(by="stock_actual", ascending=False).head(20)
        
        # Crear gráfico de barras: nombre del producto vs stock_actual
        st.bar_chart(
            top_products.set_index("nombre")["stock_actual"],
            use_container_width=True
        )
    else:
        st.info("No hay productos para mostrar con los filtros actuales.")


    # --- Gráfica ejemplo ---
    st.header("Hello world!!!")
    apps = st.slider("Number of apps", max_value=60, value=10)
    chart_data = pd.DataFrame({'y':[2 ** x for x in range(apps)]})
    st.bar_chart(chart_data, height=500, width=min(100+50*apps, 1000), 
                 use_container_width=False, x_label="Apps", y_label="Fun with data")


# COMMAND ----------

# MAGIC %md
# MAGIC # Personalización: Logo, colores y texto introductorio
# MAGIC
# MAGIC ## 📁 Estructura de carpetas para el logo
# MAGIC
# MAGIC Para agregar el logo de tu compañía, crea la siguiente estructura de carpetas:
# MAGIC
# MAGIC ```
# MAGIC /app
# MAGIC    ├── app.py
# MAGIC    ├── assets/
# MAGIC    │      └── logo.png
# MAGIC ```
# MAGIC
# MAGIC **Pasos:**
# MAGIC 1. Crea una carpeta llamada `assets` dentro del directorio `/app`
# MAGIC 2. Guarda el logo de tu compañía como `logo.png` dentro de la carpeta `assets`
# MAGIC 3. El código leerá automáticamente el logo desde esta ubicación
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎨 Código de personalización
# MAGIC
# MAGIC Copia y pega este código en tu `app.py` para agregar el logo de tu compañía:
# MAGIC
# MAGIC ```python
# MAGIC # --- Logo de la compañía ---
# MAGIC st.image("assets/logo.png", width=180)  # Lee el logo desde la carpeta assets
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Instrucciones:**
# MAGIC * Coloca este código después de `st.set_page_config()` y antes del `st.title()`
# MAGIC * Ajusta el parámetro `width` para cambiar el tamaño del logo (ej: 150, 200, 250)
# MAGIC * Si prefieres usar una URL externa, reemplaza `"assets/logo.png"` por la URL completa de tu logo

# COMMAND ----------

# MAGIC %md
# MAGIC # Personalización: Colores y texto introductorio
# MAGIC
# MAGIC ## 🎨 Lista de 10 colores comunes para personalizar la app
# MAGIC
# MAGIC Copia y pega este código en tu `app.py` para personalizar los colores y el texto de bienvenida:
# MAGIC
# MAGIC **Ubicación en tu `app.py`:**
# MAGIC
# MAGIC Pega este código **después de `st.set_page_config()` y antes de cargar los datos**. Esto asegura que los estilos y el texto de bienvenida aparezcan al inicio de tu app.
# MAGIC
# MAGIC ```python
# MAGIC # --- Lista de colores comunes ---
# MAGIC st.markdown("""
# MAGIC **Colores comunes (elige el código HEX para tu app):**
# MAGIC
# MAGIC 1. Azul oscuro: `#1a237e`
# MAGIC 2. Azul claro: `#42a5f5`
# MAGIC 3. Verde: `#43a047`
# MAGIC 4. Rojo: `#e53935`
# MAGIC 5. Naranja: `#fb8c00`
# MAGIC 6. Amarillo: `#fdd835`
# MAGIC 7. Gris claro: `#f5f7fa`
# MAGIC 8. Gris oscuro: `#424242`
# MAGIC 9. Morado: `#8e24aa`
# MAGIC 10. Negro: `#212121`
# MAGIC """)
# MAGIC
# MAGIC # --- Colores personalizados (puedes cambiar los valores HEX) ---
# MAGIC st.markdown(
# MAGIC     """
# MAGIC     <style>
# MAGIC     .main {
# MAGIC         background-color: #f5f7fa;
# MAGIC     }
# MAGIC     .stApp {
# MAGIC         background-color: #f5f7fa;
# MAGIC     }
# MAGIC     .css-18e3th9 {
# MAGIC         background-color: #ffffff;
# MAGIC         border-radius: 10px;
# MAGIC         padding: 20px;
# MAGIC     }
# MAGIC     .st-bb {
# MAGIC         color: #1a237e;
# MAGIC     }
# MAGIC     .st-cb {
# MAGIC         color: #3949ab;
# MAGIC     }
# MAGIC     </style>
# MAGIC     """,
# MAGIC     unsafe_allow_html=True
# MAGIC )
# MAGIC
# MAGIC # --- Texto explicativo en español ---
# MAGIC st.markdown("""
# MAGIC ### Bienvenido a la App de Inventario de Insumos de Oficina
# MAGIC
# MAGIC Esta aplicación te permite visualizar y actualizar en tiempo real el inventario de insumos de oficina de la compañía. 
# MAGIC Podrás filtrar por categoría y subcategoría, actualizar el stock de productos y consultar insights usando el chatbot integrado.
# MAGIC
# MAGIC ¡Comienza seleccionando tu conexión y explora las funcionalidades disponibles!
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Instrucciones:**
# MAGIC * Cambia los valores HEX (ej: `#f5f7fa`) por los colores de tu preferencia
# MAGIC * Modifica el texto de bienvenida según tu caso de uso
# MAGIC * Agrega este código después de `st.set_page_config()` y antes de la lógica principal de tu app

# COMMAND ----------

# MAGIC %md
# MAGIC # -----------------------------
# MAGIC # Instrucciones para agregar el chatbot Genie a la app
# MAGIC # -----------------------------
# MAGIC
# MAGIC ### 🤖 Instrucciones para agregar el chatbot Genie
# MAGIC
# MAGIC 1. **Configura tu Genie Space en Databricks:**
# MAGIC    - Ve a la sección de Genie en tu workspace de Databricks.
# MAGIC    - Crea un nuevo espacio Genie o usa uno existente.
# MAGIC    - Copia el `Genie Space ID` que se mostrará en la configuración.
# MAGIC
# MAGIC 2. **Agrega tu Genie Space ID en el código:**
# MAGIC    - Busca la variable `genie_space_id` en el código.
# MAGIC    - Reemplaza `"Genie_ID"` por el ID real de tu espacio Genie.
# MAGIC
# MAGIC 3. **Asegúrate de tener permisos y el token configurado:**
# MAGIC    - Debes tener configuradas las variables de entorno `DATABRICKS_HOST` y `DATABRICKS_TOKEN` para la autenticación.
# MAGIC
# MAGIC 4. **Utiliza el chat en la app:**
# MAGIC    - Escribe tu pregunta en el cuadro de chat en la parte inferior de la app.
# MAGIC    - El asistente responderá usando Genie, mostrando resultados y código SQL generado si aplica.
# MAGIC
# MAGIC 5. **Personaliza las instrucciones de Genie (opcional):**
# MAGIC    - En la configuración de tu espacio Genie, puedes agregar instrucciones específicas en español para mejorar las respuestas del asistente.
# MAGIC
# MAGIC > **Nota:** Si tienes problemas con la conexión o el ID, revisa la configuración y permisos de tu workspace.
# MAGIC
# MAGIC ---

# COMMAND ----------


# -----------------------------         ######
# Configuración Workspace
# -----------------------------
w = WorkspaceClient()
genie_space_id = "Genie_ID"  configura tu genie ID ######


# -----------------------------
# Indicador para el chat
# -----------------------------
st.markdown("---")  # Línea separadora opcional
st.subheader("💬 Habla con tus datos / Pregunta insights")

# -----------------------------
# Chat con Genie
# -----------------------------

def display_message(message):
    if "content" in message:
        st.markdown(message["content"])
    if "data" in message:
        st.dataframe(message["data"])
    if "code" in message:
        with st.expander("Show generated code"):
            st.code(message["code"], language="sql", wrap_lines=True)


def get_query_result(statement_id):
    # For simplicity, let's say data fits in one chunk, query.manifest.total_chunk_count = 1

    result = w.statement_execution.get_statement(statement_id)
    return pd.DataFrame(
        result.result.data_array, columns=[i.name for i in result.manifest.schema.columns]
    )


def process_genie_response(response):
    for i in response.attachments:
        if i.text:
            message = {"role": "assistant", "content": i.text.content}
            display_message(message)
        elif i.query:
            data = get_query_result(response.query_result.statement_id)
            message = {
                "role": "assistant", "content": i.query.description, "data": data, "code": i.query.query
            }
            display_message(message)


if prompt := st.chat_input("Ask your question..."):
    # Refer to actual app code for chat history persistence on rerun

    st.chat_message("user").markdown(prompt)

    with st.chat_message("assistant"):
        if st.session_state.get("conversation_id"):
            conversation = w.genie.create_message_and_wait(
                genie_space_id, st.session_state.conversation_id, prompt
            )
            process_genie_response(conversation)
        else:
            conversation = w.genie.start_conversation_and_wait(genie_space_id, prompt)
            process_genie_response(conversation)

