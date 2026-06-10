# Comfama — Sesión Express: Lakebase

**Duración:** 1 hora · **Tipo:** Tour interactivo · **Audiencia:** Equipo IA / Plataforma Comfama

Introducción **surface-level** a Lakebase. El workshop deep-dive hands-on viene a fin de mes (unified — Apps + Agents + Lakebase).

---

## Objetivo de esta hora

> Entender qué es Lakebase, por qué reemplaza Postgres/Cosmos para casos OLTP de agentes, y dejar las preguntas listas para el workshop.

## Por qué importa para Comfama

Hoy ustedes usan **Cosmos DB** para el estado conversacional del agente (parte del Framework custom en Azure). Lakebase reemplaza esa pieza con Postgres serverless gestionado, sync nativo a Delta, branching para dev/test, scale-to-zero.

## Productos que vamos a tocar

| Producto | Profundidad hoy |
|---|---|
| **Lakebase** (concepto + arquitectura) | 🟢 Slides + connection demo |
| **Branching** | 🟡 Concepto + comando |
| **Sync Delta ↔ Lakebase** | 🟡 Concepto |

---

## Agenda (60 min)

| Tiempo | Notebook |
|---|---|
| 0–5 | `00 - Bienvenida y Agenda` |
| 5–25 | `01 - Product Tour (Slides)` — 16 slides del Lakebase deck (español) |
| 25–50 | `02 - LAB Express` — Crear/conectar a una instancia, queries básicos, branching |
| 50–60 | `03 - Cierre y Workshop Preview` |

---

## Estructura

```
Lakebase/
├── README.md
├── imagenes/                              (16 slides en español)
├── 00 - Bienvenida y Agenda
├── 01 - Product Tour (Slides)
├── 02 - LAB Express
└── 03 - Cierre y Workshop Preview
```
