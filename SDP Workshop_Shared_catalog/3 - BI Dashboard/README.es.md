## Dashboard con filtros globales y drill‑down (Catálogo Compartido)

Este ejercicio usa el mismo catálogo compartido para todos, con esquemas por usuario. Agregaremos:
- Filtro global por `order_date` y, opcionalmente, `city`.
- Interacciones de drill‑down (detalle por fecha y/o ciudad).
- Genie en español.

### 1) Catálogo y variables
```sql
USE CATALOG ${catalog}; -- catálogo compartido
SET bronze = 'sdp_workshop_${clean_username}_bronze';
SET silver = 'sdp_workshop_${clean_username}_silver';
SET gold   = 'sdp_workshop_${clean_username}_gold';
```

### 2) Queries
Usa `kpis.sql` para:
- KPIs base (total, tendencia, promedio).
- Detalle por `order_date`.
- (Opcional) consultas por `city` si tienes `${silver}.customers`.

### 3) Filtros globales del dashboard
- Filtro de rango de fechas → Field: `order_date`, mapea a tiles con ese campo.
- (Opcional) Filtro de ciudad → Field: `city`, mapea a tiles que expongan `city`.

### 4) Drill‑down
- En el gráfico de “Pedidos por día”: Add interaction → Cross‑filter → aplica `order_date` al tile “Detalle de pedidos”.
- (Opcional) Drill‑through a otro dashboard y pasa filtros (`order_date`, `city`).

### 5) Genie
- Actívalo en Settings del dashboard y realiza preguntas en español.

Consulta `kpis.sql` para las consultas base y de detalle.


