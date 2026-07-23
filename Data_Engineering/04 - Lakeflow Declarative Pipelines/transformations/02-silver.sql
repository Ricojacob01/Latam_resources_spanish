-- ==========================================================================
-- SILVER: Limpieza, validación y enriquecimiento de datos bancarios
-- ==========================================================================

-- Transacciones validadas
CREATE OR REFRESH STREAMING TABLE silver.transacciones (
  fecha_transaccion DATE COMMENT "Fecha de la transacción.",
  transaccion_id STRING COMMENT "Identificador único de la transacción.",
  cuenta_id STRING COMMENT "Cuenta asociada.",
  cliente_id STRING COMMENT "Cliente asociado.",
  fecha_hora TIMESTAMP COMMENT "Timestamp de la transacción.",
  monto DECIMAL(19,2) COMMENT "Monto en colones (CRC).",
  moneda STRING COMMENT "Moneda de la transacción.",
  tipo_transaccion STRING COMMENT "Tipo: deposito, retiro, transferencia, pago, comision.",
  canal STRING COMMENT "Canal: app_movil, sucursal, atm, web, sinpe.",
  sucursal_id STRING COMMENT "Sucursal donde se originó la transacción.",
  producto STRING COMMENT "Producto bancario asociado.",
  CONSTRAINT monto_positivo EXPECT(monto > 0) ON VIOLATION DROP ROW,
  CONSTRAINT canal_valido EXPECT(canal IN ('app_movil', 'sucursal', 'atm', 'web', 'sinpe')) ON VIOLATION DROP ROW
)
COMMENT "Transacciones bancarias validadas y enriquecidas."
AS SELECT
  DATE(fecha_hora) AS fecha_transaccion,
  transaccion_id,
  cuenta_id,
  cliente_id,
  CAST(fecha_hora AS TIMESTAMP) AS fecha_hora,
  CAST(monto AS DECIMAL(19,2)) AS monto,
  moneda,
  tipo_transaccion,
  canal,
  sucursal_id,
  producto
FROM STREAM(bronze.transacciones_raw);

-- Cuentas limpias
CREATE OR REFRESH STREAMING TABLE silver.cuentas (
  cuenta_id STRING COMMENT "Identificador de la cuenta.",
  cliente_id STRING COMMENT "Cliente titular.",
  tipo_cuenta STRING COMMENT "corriente o ahorro.",
  saldo DECIMAL(19,2) COMMENT "Saldo actual.",
  fecha_apertura DATE COMMENT "Fecha de apertura.",
  estado STRING COMMENT "activa o inactiva.",
  CONSTRAINT cuenta_activa EXPECT(estado = 'activa') ON VIOLATION DROP ROW
)
COMMENT "Cuentas bancarias validadas."
AS SELECT
  cuenta_id,
  cliente_id,
  tipo_cuenta,
  CAST(saldo AS DECIMAL(19,2)) AS saldo,
  CAST(fecha_apertura AS DATE) AS fecha_apertura,
  estado
FROM STREAM(bronze.cuentas_raw);

-- Sucursales
CREATE OR REFRESH STREAMING TABLE silver.sucursales  (
  sucursal_id STRING COMMENT "Código de sucursal.",
  nombre STRING COMMENT "Nombre de la sucursal.",
  provincia STRING COMMENT "Provincia de Costa Rica.",
  region STRING COMMENT "Región geográfica."
)
COMMENT "Catálogo de sucursales BNCR."
AS SELECT
  sucursal_id,
  nombre,
  provincia,
  region
FROM STREAM(bronze.sucursales_raw);

-- AUTO CDC: Clientes (SCD Tipo 2)
CREATE OR REFRESH STREAMING TABLE silver.clientes (
  customer_id STRING COMMENT "Identificador único del cliente.",
  nombre STRING COMMENT "Nombre completo.",
  cedula STRING COMMENT "Cédula de identidad.",
  segmento STRING COMMENT "Segmento: retail, premium, empresarial.",
  email STRING COMMENT "Correo electrónico.",
  telefono STRING COMMENT "Teléfono de contacto."
)
COMMENT "Clientes con historial SCD Tipo 2.";

CREATE FLOW clientes_cdc_flow AS AUTO CDC INTO silver.clientes
FROM STREAM(bronze.clientes_cdc_raw)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY to_timestamp(event_timestamp, 'MM-dd-yyyy HH:mm:ss')
COLUMNS * EXCEPT (operation, event_timestamp, file_name, _rescued_data)
STORED AS SCD TYPE 2;
