# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 00 - Generacion de Imagenes de Displays
# MAGIC
# MAGIC Este notebook genera imagenes sinteticas de displays de productos utilizando
# MAGIC el **Foundation Model API** de Databricks para crear el dataset de referencia del workshop.
# MAGIC </br></br>
# MAGIC
# MAGIC ## Objetivos
# MAGIC
# MAGIC - Generar ~20 imagenes sinteticas de displays de productos en 4 categorias de calidad
# MAGIC - Almacenar las imagenes en un Volumen de Unity Catalog
# MAGIC - Preparar el dataset listo para usar en los siguientes laboratorios
# MAGIC
# MAGIC **Nota:** Este notebook solo necesita ejecutarse **una vez** antes del workshop.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparacion
# MAGIC
# MAGIC 1. En la esquina superior derecha, haga clic en **Connect**
# MAGIC 2. Seleccione el cluster: **Serverless**

# COMMAND ----------

# MAGIC %run "../config"

# COMMAND ----------

import requests
import json
import base64
import time
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 01 - Crear esquema y volumen

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"Schema '{CATALOG}.{SCHEMA}' y volumen '{VOLUME}' creados/verificados.")

# COMMAND ----------

# Crear subdirectorio para displays
dbutils.fs.mkdirs(f"{PATH_VOLUME}/displays")
print(f"Directorio creado: {PATH_VOLUME}/displays")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 02 - Configurar la generacion de imagenes
# MAGIC
# MAGIC Usaremos el endpoint de **Databricks Foundation Model API** para generar imagenes.
# MAGIC
# MAGIC Si tu workspace no tiene acceso a un modelo de generacion de imagenes,
# MAGIC puedes usar la **Opcion B** al final de este notebook que descarga imagenes
# MAGIC de ejemplo desde URLs publicas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Definir los prompts por categoria de calidad
# MAGIC
# MAGIC Cada categoria de display tiene caracteristicas visuales distintas:
# MAGIC
# MAGIC | Categoria | Caracteristicas visuales |
# MAGIC |-----------|-------------------------|
# MAGIC | **Excelente** | Productos bien organizados, estantes llenos, senalizacion clara, buena iluminacion |
# MAGIC | **Bueno** | Mayormente organizado, algunos espacios vacios, senalizacion presente |
# MAGIC | **Regular** | Parcialmente desordenado, varios espacios vacios, senalizacion minima |
# MAGIC | **Deficiente** | Desordenado, estantes vacios, sin senalizacion, productos caidos |

# COMMAND ----------

# DBTITLE 1,Definir prompts para cada categoria y marca
DISPLAY_PROMPTS = {
    "excelente": {
        "Coca-Cola": [
            "Professional retail product display shelf with perfectly organized Coca-Cola bottles and cans, fully stocked shelves, clean red branding signage, bright supermarket lighting, merchandising excellence, commercial photography",
            "Impeccable convenience store Coca-Cola refrigerator display, all products facing forward, price tags aligned, promotional materials visible, professional retail photography"
        ],
        "PepsiCo": [
            "Perfectly arranged PepsiCo beverage display in supermarket aisle, full shelves with Pepsi Gatorade and Lay's products, blue branding, clean organized retail shelf, commercial photography",
        ],
        "Nestle": [
            "Well-organized Nestle product display in supermarket, cereal boxes and chocolate bars perfectly aligned, full stock, clean shelf with brand signage, professional retail photo",
        ],
        "Bimbo": [
            "Excellent bread and bakery display in convenience store, Bimbo products neatly arranged, fresh stock fully loaded, promotional signage visible, bright retail lighting, commercial photo",
        ],
    },
    "bueno": {
        "Coca-Cola": [
            "Good retail shelf display with Coca-Cola products mostly organized, few minor gaps in stock, signage present but slightly off-center, typical supermarket aisle, realistic retail photography",
            "Convenience store Coca-Cola display, mostly full shelves, one or two products slightly turned, acceptable merchandising, natural store lighting",
        ],
        "PepsiCo": [
            "Decent PepsiCo product shelf in grocery store, mostly stocked Pepsi products, a few small gaps, adequate signage, standard retail environment, realistic photo",
        ],
        "Nestle": [
            "Reasonably organized Nestle display shelf, most products aligned, a few spaces visible, signage present, standard supermarket setting, commercial photo",
        ],
        "Bimbo": [
            "Adequate Bimbo bread display in store, mostly stocked shelves, minor disorganization, price tags visible, typical convenience store, realistic photography",
        ],
    },
    "regular": {
        "Coca-Cola": [
            "Mediocre retail display with Coca-Cola products partially disorganized, several empty spots on shelves, some products turned sideways, faded signage, dim store lighting, realistic photo",
            "Average convenience store beverage shelf with scattered Coca-Cola products, half-empty shelves, no promotional materials, cluttered appearance, realistic retail photo",
        ],
        "PepsiCo": [
            "Partially messy PepsiCo display in grocery store, mixed products, several gaps, minimal signage, average retail conditions, realistic photography",
        ],
        "Nestle": [
            "Below average Nestle shelf display, products out of order, noticeable empty spaces, dusty shelf, poor signage, standard store, realistic photo",
        ],
        "Bimbo": [
            "Mediocre bread display with Bimbo products partially stocked, some packages crushed, minimal organization, dim lighting, realistic convenience store photo",
        ],
    },
    "deficiente": {
        "Coca-Cola": [
            "Poorly maintained retail display with scattered Coca-Cola products, mostly empty shelves, fallen bottles, no signage, dirty shelf, neglected store section, realistic photo",
            "Terrible convenience store beverage display, nearly empty Coca-Cola shelf, products fallen over, broken price tags, dust visible, poor lighting, realistic retail photo",
        ],
        "PepsiCo": [
            "Very messy and neglected PepsiCo display, empty shelves, products on wrong shelves, no branding visible, dirty retail environment, realistic photography",
        ],
        "Nestle": [
            "Abandoned-looking Nestle shelf, almost empty, remaining products disorganized and dusty, torn labels, broken signage, poorly lit store section, realistic photo",
        ],
        "Bimbo": [
            "Terrible bread display, Bimbo products expired-looking and crushed, nearly empty shelf, dirty surface, no price tags, neglected convenience store, realistic photo",
        ],
    },
}

# Contar total de imagenes
total = sum(
    len(prompts)
    for quality in DISPLAY_PROMPTS.values()
    for prompts in quality.values()
)
print(f"Total de imagenes a generar: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 03 - Opcion A: Generar imagenes con Foundation Model API
# MAGIC
# MAGIC Usamos el endpoint de generacion de imagenes disponible en el workspace.

# COMMAND ----------

# DBTITLE 1,Funcion para generar imagen via API
def generate_image_fmapi(prompt: str, filename: str, save_path: str) -> str:
    """
    Genera una imagen usando el Foundation Model API de Databricks.

    Args:
        prompt: Descripcion de la imagen a generar
        filename: Nombre del archivo de salida
        save_path: Ruta del volumen donde guardar

    Returns:
        Ruta completa del archivo guardado
    """
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    host = spark.conf.get("spark.databricks.workspaceUrl")

    # Usar el endpoint de generacion de imagenes
    # Opciones comunes: databricks-shutterstock-imageai, databricks-meta-llama-image, etc.
    url = f"https://{host}/serving-endpoints/databricks-meta-llama-4-maverick/invocations"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": f"Generate an image: {prompt}"
                    }
                ]
            }
        ],
        "max_tokens": 4096
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        result = response.json()

        # Extraer imagen base64 de la respuesta si disponible
        content = result.get("choices", [{}])[0].get("message", {}).get("content", "")

        # Si el modelo devuelve una imagen en base64
        if "base64" in str(result).lower():
            # Parsear la imagen base64 de la respuesta
            for choice in result.get("choices", []):
                msg = choice.get("message", {})
                if isinstance(msg.get("content"), list):
                    for block in msg["content"]:
                        if block.get("type") == "image_url":
                            img_data = block["image_url"]["url"]
                            if img_data.startswith("data:image"):
                                img_data = img_data.split(",", 1)[1]
                            img_bytes = base64.b64decode(img_data)
                            filepath = f"{save_path}/{filename}"
                            with open(filepath.replace("/Volumes", "/Volumes"), "wb") as f:
                                f.write(img_bytes)
                            return filepath

        print(f"  Nota: El modelo no devolvio imagen directamente para {filename}")
        return None

    except Exception as e:
        print(f"  Error generando {filename}: {e}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Intentar generar con Foundation Model API
# MAGIC
# MAGIC **Si este paso falla** (por ejemplo, si el modelo no soporta generacion de imagenes),
# MAGIC pase directamente a la **Opcion B** que usa imagenes sinteticas generadas por codigo.

# COMMAND ----------

# DBTITLE 1,Intentar generacion via FMAPI
generated_files = []
display_id = 1
save_dir = f"{PATH_VOLUME}/displays"
use_fallback = False

for quality, brands in DISPLAY_PROMPTS.items():
    for brand, prompts in brands.items():
        for prompt in prompts:
            filename = f"display_{display_id:03d}.jpg"
            print(f"Generando display_{display_id:03d} | {quality} | {brand}...")

            result = generate_image_fmapi(prompt, filename, save_dir)
            if result:
                generated_files.append({
                    "display_id": display_id,
                    "filename": filename,
                    "quality": quality,
                    "brand": brand,
                    "path": result
                })
            else:
                print(f"  >> Fallback necesario para {filename}")
                use_fallback = True
                break

            display_id += 1
            time.sleep(1)  # Rate limiting

        if use_fallback:
            break
    if use_fallback:
        break

if use_fallback:
    print("\n** La generacion via FMAPI no esta disponible. Ejecute la Opcion B a continuacion. **")
else:
    print(f"\nGeneradas {len(generated_files)} imagenes exitosamente!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 04 - Opcion B: Imagenes sinteticas generadas por codigo (Fallback)
# MAGIC
# MAGIC Si el Foundation Model API no soporta generacion de imagenes en este workspace,
# MAGIC generamos imagenes sinteticas con **Pillow** que simulan displays de distintas calidades.
# MAGIC
# MAGIC Estas imagenes usan colores, patrones y texto para representar visualmente
# MAGIC la calidad de cada display. Son suficientes para demostrar el flujo completo del workshop.

# COMMAND ----------

# DBTITLE 1,Generar imagenes sinteticas con Pillow
# MAGIC %pip install Pillow --quiet

# COMMAND ----------

from PIL import Image, ImageDraw, ImageFont
import random
import struct

def generate_synthetic_display(display_id: int, brand: str, quality: str,
                                store_type: str, save_path: str) -> str:
    """
    Genera una imagen sintetica de un display de productos.

    Los colores y patrones representan la calidad:
    - Excelente: colores vibrantes, patron ordenado, estantes llenos
    - Bueno: colores claros, mayormente ordenado
    - Regular: colores apagados, parcialmente desordenado
    - Deficiente: colores oscuros, desordenado, espacios vacios
    """
    width, height = 640, 480
    random.seed(display_id * 42)

    # Colores base por calidad
    bg_colors = {
        "excelente": (240, 248, 255),   # Azul claro - limpio
        "bueno": (245, 245, 220),       # Beige - aceptable
        "regular": (220, 220, 200),     # Gris claro - mediocre
        "deficiente": (180, 170, 160),  # Gris oscuro - descuidado
    }

    brand_colors = {
        "Coca-Cola": (220, 20, 20),
        "PepsiCo": (0, 70, 170),
        "Nestle": (0, 120, 180),
        "Bimbo": (0, 100, 50),
    }

    # Parametros por calidad
    fill_ratio = {"excelente": 0.95, "bueno": 0.80, "regular": 0.55, "deficiente": 0.25}
    disorder = {"excelente": 2, "bueno": 5, "regular": 15, "deficiente": 30}

    img = Image.new("RGB", (width, height), bg_colors.get(quality, (200, 200, 200)))
    draw = ImageDraw.Draw(img)

    bc = brand_colors.get(brand, (100, 100, 100))

    # -- Dibujar estantes --
    num_shelves = 4
    shelf_height = height // (num_shelves + 1)

    for shelf in range(num_shelves):
        y_base = 60 + shelf * shelf_height

        # Linea del estante
        shelf_color = (139, 119, 101) if quality != "deficiente" else (100, 85, 70)
        draw.rectangle([20, y_base + shelf_height - 8, width - 20, y_base + shelf_height],
                       fill=shelf_color)

        # Productos en el estante
        num_slots = 10
        slot_width = (width - 60) // num_slots

        for slot in range(num_slots):
            # Decidir si hay producto (basado en fill_ratio)
            if random.random() > fill_ratio[quality]:
                continue

            x = 30 + slot * slot_width
            prod_height = random.randint(shelf_height // 3, shelf_height - 15)
            y_top = y_base + shelf_height - 8 - prod_height

            # Desplazamiento por desorden
            dx = random.randint(-disorder[quality], disorder[quality])
            dy = random.randint(-disorder[quality] // 2, disorder[quality] // 2)

            # Color del producto (variaciones del color de marca)
            r = max(0, min(255, bc[0] + random.randint(-30, 30)))
            g = max(0, min(255, bc[1] + random.randint(-30, 30)))
            b = max(0, min(255, bc[2] + random.randint(-30, 30)))

            # Dibujar producto
            draw.rectangle(
                [x + dx, y_top + dy, x + slot_width - 4 + dx, y_base + shelf_height - 10 + dy],
                fill=(r, g, b),
                outline=(50, 50, 50),
                width=1
            )

            # Etiqueta pequena en producto (simulando label)
            if quality in ("excelente", "bueno"):
                label_y = y_top + dy + prod_height // 3
                draw.rectangle(
                    [x + dx + 2, label_y, x + slot_width - 6 + dx, label_y + 8],
                    fill=(255, 255, 255)
                )

    # -- Senalizacion de marca --
    if quality in ("excelente", "bueno"):
        # Banner superior
        draw.rectangle([20, 10, width - 20, 50], fill=bc)
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 22)
        except:
            font = ImageFont.load_default()
        draw.text((width // 2 - 60, 15), brand, fill=(255, 255, 255), font=font)
    elif quality == "regular":
        # Banner parcial
        draw.rectangle([20, 10, width // 2, 45], fill=(*bc, 150))
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 16)
        except:
            font = ImageFont.load_default()
        draw.text((30, 15), brand, fill=(255, 255, 255), font=font)

    # -- Efecto de iluminacion --
    if quality == "excelente":
        # Brillo uniforme
        for i in range(0, width, 80):
            draw.line([(i, 0), (i, 5)], fill=(255, 255, 200), width=2)
    elif quality == "deficiente":
        # Sombras
        overlay = Image.new("RGBA", (width, height), (0, 0, 0, 40))
        img = Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")
        draw = ImageDraw.Draw(img)

    # -- Etiqueta de info (esquina inferior) --
    try:
        small_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 11)
    except:
        small_font = ImageFont.load_default()
    info_text = f"ID:{display_id} | {brand} | {store_type} | {quality.upper()}"
    draw.rectangle([0, height - 20, width, height], fill=(0, 0, 0))
    draw.text((5, height - 18), info_text, fill=(200, 200, 200), font=small_font)

    # Guardar
    filename = f"display_{display_id:03d}.jpg"
    filepath = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/displays/{filename}"
    img.save(filepath, "JPEG", quality=85)

    return filepath

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generar todas las imagenes

# COMMAND ----------

# DBTITLE 1,Generar 20 imagenes sinteticas
# Definicion del dataset completo
display_definitions = [
    # -- Excelentes --
    {"display_id": 1,  "brand": "Coca-Cola", "store_type": "supermercado",  "region": "CDMX",        "quality": "excelente", "ideal": 95.0, "compliance": 92.0},
    {"display_id": 2,  "brand": "Coca-Cola", "store_type": "conveniencia",  "region": "Monterrey",    "quality": "excelente", "ideal": 90.0, "compliance": 88.0},
    {"display_id": 3,  "brand": "PepsiCo",   "store_type": "supermercado",  "region": "Guadalajara",  "quality": "excelente", "ideal": 93.0, "compliance": 90.0},
    {"display_id": 4,  "brand": "Nestle",    "store_type": "supermercado",  "region": "CDMX",         "quality": "excelente", "ideal": 91.0, "compliance": 89.0},
    {"display_id": 5,  "brand": "Bimbo",     "store_type": "conveniencia",  "region": "Puebla",       "quality": "excelente", "ideal": 88.0, "compliance": 91.0},
    # -- Buenos --
    {"display_id": 6,  "brand": "Coca-Cola", "store_type": "supermercado",  "region": "Monterrey",    "quality": "bueno", "ideal": 78.0, "compliance": 75.0},
    {"display_id": 7,  "brand": "PepsiCo",   "store_type": "conveniencia",  "region": "CDMX",         "quality": "bueno", "ideal": 80.0, "compliance": 77.0},
    {"display_id": 8,  "brand": "Nestle",    "store_type": "supermercado",  "region": "Guadalajara",  "quality": "bueno", "ideal": 76.0, "compliance": 80.0},
    {"display_id": 9,  "brand": "Bimbo",     "store_type": "supermercado",  "region": "Monterrey",    "quality": "bueno", "ideal": 82.0, "compliance": 78.0},
    {"display_id": 10, "brand": "Coca-Cola", "store_type": "conveniencia",  "region": "Puebla",       "quality": "bueno", "ideal": 74.0, "compliance": 72.0},
    # -- Regulares --
    {"display_id": 11, "brand": "PepsiCo",   "store_type": "supermercado",  "region": "CDMX",         "quality": "regular", "ideal": 60.0, "compliance": 55.0},
    {"display_id": 12, "brand": "Nestle",    "store_type": "conveniencia",  "region": "Monterrey",    "quality": "regular", "ideal": 58.0, "compliance": 62.0},
    {"display_id": 13, "brand": "Coca-Cola", "store_type": "supermercado",  "region": "Guadalajara",  "quality": "regular", "ideal": 55.0, "compliance": 50.0},
    {"display_id": 14, "brand": "Bimbo",     "store_type": "conveniencia",  "region": "Puebla",       "quality": "regular", "ideal": 62.0, "compliance": 58.0},
    {"display_id": 15, "brand": "PepsiCo",   "store_type": "supermercado",  "region": "CDMX",         "quality": "regular", "ideal": 57.0, "compliance": 53.0},
    # -- Deficientes --
    {"display_id": 16, "brand": "Coca-Cola", "store_type": "conveniencia",  "region": "Monterrey",    "quality": "deficiente", "ideal": 35.0, "compliance": 30.0},
    {"display_id": 17, "brand": "Nestle",    "store_type": "supermercado",  "region": "Guadalajara",  "quality": "deficiente", "ideal": 28.0, "compliance": 25.0},
    {"display_id": 18, "brand": "PepsiCo",   "store_type": "conveniencia",  "region": "CDMX",         "quality": "deficiente", "ideal": 32.0, "compliance": 35.0},
    {"display_id": 19, "brand": "Bimbo",     "store_type": "supermercado",  "region": "Puebla",       "quality": "deficiente", "ideal": 40.0, "compliance": 38.0},
    {"display_id": 20, "brand": "Coca-Cola", "store_type": "supermercado",  "region": "Monterrey",    "quality": "deficiente", "ideal": 25.0, "compliance": 22.0},
]

print(f"Generando {len(display_definitions)} imagenes sinteticas...\n")

generated = []
for d in display_definitions:
    filepath = generate_synthetic_display(
        display_id=d["display_id"],
        brand=d["brand"],
        quality=d["quality"],
        store_type=d["store_type"],
        save_path=PATH_VOLUME
    )
    generated.append({**d, "image_url": filepath})
    print(f"  [OK] display_{d['display_id']:03d}.jpg | {d['brand']:<10} | {d['store_type']:<14} | {d['quality']}")

print(f"\n{len(generated)} imagenes generadas en {PATH_VOLUME}/displays/")

# COMMAND ----------

# DBTITLE 1,Testing images intro
# MAGIC %md
# MAGIC ## Ejercicio 04b - Generar imagenes de prueba (Testing)
# MAGIC
# MAGIC Generamos 2 imagenes adicionales para testing:
# MAGIC - **display_test_good.jpg** — display de calidad "bueno"
# MAGIC - **display_test_bad.jpg** — display de calidad "deficiente"
# MAGIC
# MAGIC Estas imagenes se guardan en un subdirectorio separado (`/displays/testing/`).

# COMMAND ----------

# DBTITLE 1,Generar 2 imagenes de testing
# Crear subdirectorio para testing
test_dir = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/displays/testing"
dbutils.fs.mkdirs(f"{PATH_VOLUME}/displays/testing")

# Definir las 2 imagenes de prueba
test_definitions = [
    {"display_id": 21, "brand": "Coca-Cola", "store_type": "supermercado", "quality": "bueno",       "filename": "display_test_good.jpg"},
    {"display_id": 22, "brand": "PepsiCo",   "store_type": "conveniencia", "quality": "deficiente", "filename": "display_test_bad.jpg"},
]

print("Generando imagenes de testing...\n")
test_generated = []
for t in test_definitions:
    filepath = generate_synthetic_display(
        display_id=t["display_id"],
        brand=t["brand"],
        quality=t["quality"],
        store_type=t["store_type"],
        save_path=test_dir
    )
    # Move file to testing subdirectory with custom name
    final_path = f"{test_dir}/{t['filename']}"
    dbutils.fs.mv(filepath, final_path)
    test_generated.append({**t, "path": final_path})
    print(f"  {t['filename']:<30} | {t['quality']:<12} | {t['brand']}")

print(f"\n{len(test_generated)} imagenes de testing generadas en {test_dir}/")

# Visualizar
import matplotlib.pyplot as plt
from PIL import Image

fig, axes = plt.subplots(1, 2, figsize=(12, 5))
for ax, t in zip(axes, test_generated):
    img = Image.open(t["path"])
    ax.imshow(img)
    ax.set_title(f"{t['filename']}\n({t['quality'].upper()})", fontsize=13, fontweight="bold")
    ax.axis("off")
plt.suptitle("Imagenes de Testing", fontsize=15, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 05 - Verificar las imagenes generadas

# COMMAND ----------

# DBTITLE 1,Listar archivos generados
files = dbutils.fs.ls(f"{PATH_VOLUME}/displays/")
print(f"Archivos en {PATH_VOLUME}/displays/:")
print("-" * 60)
for f in files:
    print(f"  {f.name:<25} | {f.size:>8} bytes")
print(f"\nTotal: {len(files)} archivos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizar ejemplos de cada categoria
# MAGIC
# MAGIC Vamos a ver un ejemplo de cada categoria para validar que las imagenes se ven correctas.

# COMMAND ----------

# DBTITLE 1,Mostrar una imagen de ejemplo por categoria
import matplotlib.pyplot as plt
from PIL import Image

fig, axes = plt.subplots(1, 4, figsize=(20, 5))

samples = [
    (1, "Excelente"),
    (6, "Bueno"),
    (11, "Regular"),
    (16, "Deficiente"),
]

for ax, (display_id, label) in zip(axes, samples):
    filepath = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/displays/display_{display_id:03d}.jpg"
    try:
        img = Image.open(filepath)
        ax.imshow(img)
        ax.set_title(f"{label}\n(display_{display_id:03d})", fontsize=14, fontweight="bold")
    except Exception as e:
        ax.text(0.5, 0.5, f"Error:\n{e}", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(label, fontsize=14)
    ax.axis("off")

plt.suptitle("Ejemplos de Displays por Categoria de Calidad", fontsize=16, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 06 - Guardar metadatos como tabla Delta
# MAGIC
# MAGIC Guardamos los metadatos de las imagenes generadas para que Lab 01 pueda usarlos directamente.

# COMMAND ----------

# DBTITLE 1,Crear tabla de referencia con metadatos
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

schema = StructType([
    StructField("display_id", IntegerType(), False),
    StructField("image_url", StringType(), False),
    StructField("brand", StringType(), True),
    StructField("store_type", StringType(), True),
    StructField("region", StringType(), True),
    StructField("ideal_score", FloatType(), True),
    StructField("compliance_score", FloatType(), True),
    StructField("quality_label", StringType(), True),
])

rows = [
    (d["display_id"], d["image_url"], d["brand"], d["store_type"],
     d["region"], d["ideal"], d["compliance"], d["quality"])
    for d in generated
]

df = spark.createDataFrame(rows, schema=schema)
df.write.mode("overwrite").saveAsTable(f"{PATH_TABLE}.displays_referencia")

spark.sql(f"""
    ALTER TABLE {PATH_TABLE}.displays_referencia
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print(f"Tabla creada: {PATH_TABLE}.displays_referencia")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC | Resultado | Valor |
# MAGIC |-----------|-------|
# MAGIC | Imagenes generadas | 20 |
# MAGIC | Categorias | excelente (5), bueno (5), regular (5), deficiente (5) |
# MAGIC | Marcas | Coca-Cola, PepsiCo, Nestle, Bimbo |
# MAGIC | Tipos de tienda | supermercado, conveniencia |
# MAGIC | Ubicacion | {PATH_VOLUME}/displays/ |
# MAGIC | Tabla Delta | {PATH_TABLE}.displays_referencia |
# MAGIC | CDF habilitado | Si |
# MAGIC
# MAGIC ### Los datos estan listos! Ahora puede continuar con:
# MAGIC
# MAGIC [Lab 01 - Setup y Datos de Referencia]($./Lab 01 - Setup y Datos de Referencia) (si desea revisar la configuracion)
# MAGIC
# MAGIC O saltar directamente a:
# MAGIC
# MAGIC [Lab 02 - Generacion de Embeddings de Imagenes]($./Lab 02 - Generacion de Embeddings de Imagenes)
