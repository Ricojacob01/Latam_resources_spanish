-- KPIs y consultas para dashboard con filtros globales y drill-down
-- Nota: Si usas filtros del dashboard (recomendado), NO necesitas parámetros en el WHERE.
-- Los filtros del dashboard se mapean a los campos de estas consultas (order_date, city).

-- 1) Total de pedidos (silver)
SELECT COUNT(*) AS total_pedidos
FROM ${silver}.orders_clean;

-- 2) Pedidos por día (gold) - expone order_date para filtro global
SELECT
  order_date,
  total_daily_orders AS pedidos_diarios
FROM ${gold}.order_summary
ORDER BY order_date;

-- 3) Promedio de pedidos diarios (gold)
SELECT
  AVG(total_daily_orders) AS promedio_pedidos_diarios
FROM ${gold}.order_summary;

-- 4) Detalle de pedidos (silver) - para drill-down por fecha
-- Calcula order_date desde order_timestamp para que el filtro se aplique por ese campo
SELECT
  date(order_timestamp) AS order_date,
  order_id,
  customer_id
FROM ${silver}.orders_clean
ORDER BY order_date DESC, order_id;

-- 5) (Opcional) Clientes por ciudad (requiere ${silver}.customers)
-- Útil para usar un filtro global "city" y/o drill-down adicional por ciudad
SELECT
  c.city,
  COUNT(*) AS total_clientes
FROM ${silver}.customers c
GROUP BY c.city
ORDER BY total_clientes DESC
LIMIT 20;

-- 6) (Opcional) Pedidos por ciudad y día (requiere unión con customers)
-- Útil para gráficos apilados o drill-down combinado por ciudad+fecha
SELECT
  date(o.order_timestamp) AS order_date,
  c.city,
  COUNT(*) AS pedidos
FROM ${silver}.orders_clean o
JOIN ${silver}.customers c
  ON o.customer_id = c.customer_id
GROUP BY order_date, c.city
ORDER BY order_date, pedidos DESC;

-- Si prefieres usar parámetros, descomenta y adapta:
-- WHERE (date(order_timestamp) BETWEEN {{ start_date }} AND {{ end_date }})
--   AND ({{ city }} IS NULL OR c.city = {{ city }})


