-- KPIs para catálogo compartido con filtros globales y drill-down

-- 1) Total de pedidos (silver)
SELECT COUNT(*) AS total_pedidos
FROM ${silver}.orders_clean;

-- 2) Pedidos por día (gold) - expone order_date
SELECT
  order_date,
  total_daily_orders AS pedidos_diarios
FROM ${gold}.order_summary
ORDER BY order_date;

-- 3) Promedio de pedidos diarios (gold)
SELECT
  AVG(total_daily_orders) AS promedio_pedidos_diarios
FROM ${gold}.order_summary;

-- 4) Detalle de pedidos por fecha (silver) - para drill-down
SELECT
  date(order_timestamp) AS order_date,
  order_id,
  customer_id
FROM ${silver}.orders_clean
ORDER BY order_date DESC, order_id;

-- 5) (Opcional) Clientes por ciudad si existe ${silver}.customers
SELECT
  c.city,
  COUNT(*) AS total_clientes
FROM ${silver}.customers c
GROUP BY c.city
ORDER BY total_clientes DESC
LIMIT 20;

-- 6) (Opcional) Pedidos por ciudad y día (requiere unión)
SELECT
  date(o.order_timestamp) AS order_date,
  c.city,
  COUNT(*) AS pedidos
FROM ${silver}.orders_clean o
JOIN ${silver}.customers c
  ON o.customer_id = c.customer_id
GROUP BY order_date, c.city
ORDER BY order_date, pedidos DESC;


