# Ejercicio: Dashboard con filtros globales y drill‑down (SDP Workshop)

Este ejercicio amplía el dashboard de KPIs para:
- Agregar filtros globales (por fecha y, opcionalmente, por ciudad).
- Habilitar interacciones de drill‑down (detalle al hacer clic en una fecha o barra).
- Activar Genie para preguntas en lenguaje natural.

Tablas usadas:
- `${silver}.orders_clean` (obligatoria)
- `${gold}.order_summary` (obligatoria)
- `${silver}.customers` (opcional, si completaste Lección 2)

## 1) Catálogo y variables
En un cuaderno SQL:
```sql
USE CATALOG ${catalog};  -- o USE CATALOG <tu_catalogo>
SET bronze = 'sdp_workshop_${clean_username}_bronze';
SET silver = 'sdp_workshop_${clean_username}_silver';
SET gold   = 'sdp_workshop_${clean_username}_gold';
```

## 2) Consultas base y de detalle
En este directorio encontrarás `kpis.sql` con:
- KPIs de pedidos (total, tendencia, promedio).
- Consultas de detalle por fecha (para tabla drill‑down).
- (Opcional) Métricas por ciudad si existe `${silver}.customers`.

Importante para filtros y drill‑down:
- Asegúrate de que las consultas expongan campos de filtro con nombres consistentes:
  - `order_date` para fechas (desde `${gold}.order_summary` o calculado con `date(order_timestamp)`).
  - `city` (si usas la unión con `${silver}.customers`).

## 3) Crear el dashboard en DBSQL
1. Entra en la vista SQL (DBSQL) y usa un SQL Warehouse.
2. Crea y guarda las queries de `kpis.sql` (ponles nombres claros).
3. Crea un dashboard “KPIs Pedidos (SDP)”.
4. Agrega tiles:
   - Counter: Total de pedidos (query: total_pedidos)
   - Línea: Pedidos por día (query: pedidos_por_dia) con eje X = `order_date`, Y = `pedidos_diarios`
   - Counter: Promedio de pedidos diarios (query: promedio_pedidos)
   - Tabla: Detalle de pedidos (query: detalle_pedidos)
   - (Opcional) Barras: Clientes por ciudad (query: clientes_por_ciudad)

## 4) Filtros globales
Opción A (recomendada): Filtros del dashboard
1. En el dashboard, haz clic en “Add filter”.
2. Crea un filtro “Rango de fechas”:
   - Field: `order_date`
   - Aplica el mapeo a todos los tiles que tengan el campo `order_date` (p.ej., “Pedidos por día”, “Detalle de pedidos”).
3. (Opcional) Crea un filtro “Ciudad”:
   - Field: `city`
   - Mapea este filtro a tiles que expongan `city` (p.ej., “Clientes por ciudad”, “Detalle de pedidos” si incluye la unión con clientes).

Opción B: Parámetros en queries
- Alternativamente, puedes definir parámetros (p. ej. `{{start_date}}`, `{{end_date}}`, `{{city}}`) y agregarlos en WHERE. Luego, expón los parámetros en el dashboard. (Ver comentarios en `kpis.sql`.)

## 5) Drill‑down (interacciones)
Habilita un flujo donde hacer clic en un punto/fecha filtre el detalle:
1. Abre el tile “Pedidos por día” (line chart).
2. “Add interaction” > “Cross‑filter” (o “Apply filters to other tiles”).
3. Selecciona que el clic sobre un punto (fecha) aplique filtro por `order_date` al tile “Detalle de pedidos” y a cualquier otro tile relevante.
4. Guarda. Ahora, al hacer clic en una fecha de la serie, verás el detalle filtrado a esa fecha.

Opcional (drill‑through a otro dashboard):
- “Add interaction” > “Open destination” y elige “Dashboard”. Pasa el filtro `order_date` (y `city` si aplica) al dashboard de destino.

## 6) Activar Genie
1. En el dashboard: Settings > Genie > Enable.
2. Prueba preguntas:
   - “¿Cuántos pedidos hubo la última semana?”
   - “Muéstrame clientes por ciudad.”
   - “Lista de pedidos del 2024‑01‑05.”

## 7) Métricas adicionales (ideas)
- Máximo/mínimo de `pedidos_diarios`.
- Variación semana a semana.
- (CDC) clientes “activos” por ciudad o tendencia de clientes.

## Archivos
- `kpis.sql`: consultas con campos `order_date` (y `city` opcional) para filtros y drill‑down.


