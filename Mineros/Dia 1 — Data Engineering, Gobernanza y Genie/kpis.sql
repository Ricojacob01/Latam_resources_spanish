-- KPIs y consultas para dashboard con filtros globales y drill-down
-- Nota: Si usas filtros del dashboard (recomendado), NO necesitas parámetros en el WHERE.
-- Los filtros del dashboard se mapean a los campos de estas consultas (order_date, city).
--
-- MODELO DE NOMBRES: catálogo compartido `academia`, tu esquema `<tu_apellido>`.
-- Todas las tablas viven en el MISMO esquema y usan sufijos de capa.
-- Sugerencia: al inicio del editor SQL ejecuta  USE CATALOG academia; USE SCHEMA <tu_apellido>;
-- Así puedes referirte a las tablas sin prefijo (como abajo).

-- 1) Total de pedidos (silver)
SELECT COUNT(*) AS total_pedidos
FROM orders_silver;

-- 2) Pedidos por día (gold) - expone order_date para filtro global
SELECT
  order_date,
  total_daily_orders AS pedidos_diarios
FROM order_summary_gold
ORDER BY order_date;

-- 3) Promedio de pedidos diarios (gold)
SELECT
  AVG(total_daily_orders) AS promedio_pedidos_diarios
FROM order_summary_gold;

-- 4) Detalle de pedidos (silver) - para drill-down por fecha
-- Calcula order_date desde order_timestamp para que el filtro se aplique por ese campo
SELECT
  date(order_timestamp) AS order_date,
  order_id,
  customer_id
FROM orders_silver
ORDER BY order_date DESC, order_id;

-- 5) (Opcional) Clientes por ciudad (requiere customers_silver, de la Lección 3)
-- Útil para usar un filtro global "city" y/o drill-down adicional por ciudad
SELECT
  c.city,
  COUNT(*) AS total_clientes
FROM customers_silver c
GROUP BY c.city
ORDER BY total_clientes DESC
LIMIT 20;

-- 6) (Opcional) Pedidos por ciudad y día (requiere unión con customers_silver)
-- Útil para gráficos apilados o drill-down combinado por ciudad+fecha
SELECT
  date(o.order_timestamp) AS order_date,
  c.city,
  COUNT(*) AS pedidos
FROM orders_silver o
JOIN customers_silver c
  ON o.customer_id = c.customer_id
GROUP BY order_date, c.city
ORDER BY order_date, pedidos DESC;

-- Si prefieres usar parámetros, descomenta y adapta:
-- WHERE (date(order_timestamp) BETWEEN {{ start_date }} AND {{ end_date }})
--   AND ({{ city }} IS NULL OR c.city = {{ city }})
