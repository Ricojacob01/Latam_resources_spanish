# Image Scorer con Vector Search

Workshop hands-on para construir un sistema de puntuacion de displays de productos utilizando Databricks Vector Search con embeddings de imagenes.

## Material del Curso

| # | Temas |
| -- | -- |
| 00 | LAB 00 - Generacion de Imagenes (ejecutar una vez antes del workshop) |
| 01 | LAB 01 - Setup y Datos de Referencia |
| 02 | LAB 02 - Generacion de Embeddings de Imagenes |
| 03 | LAB 03 - Endpoint e Indice de Vector Search |
| 04 | LAB 04 - Consulta Puntuacion y Explicabilidad |
| 05 | LAB 05 - Evaluacion Costos y Limpieza |
| 06 | LAB 06 - App de Image Scorer (Databricks App con Streamlit) |

## Estructura

```
image_scorer/
  config                    Parametros compartidos del workshop
  README.md                 Este archivo
  datos/                    Datos de muestra
  imagenes/                 Recursos visuales
  image_scorer_app/         Codigo fuente de la Databricks App
    app.py                  Aplicacion Streamlit
    app.yaml                Configuracion de la app
    requirements.txt        Dependencias
  labs/
    Lab 00                  Generacion de imagenes sinteticas
    Lab 01                  Setup y datos de referencia en Delta
    Lab 02                  Embeddings con CLIP / Model Serving
    Lab 03                  Endpoint e indice Vector Search
    Lab 04                  Consulta, puntuacion y explicabilidad
    Lab 05                  Evaluacion, costos y limpieza
    Lab 06                  Despliegue de Databricks App
```
