# Resumen de Actualizaciones del Taller

## Descripción general

Reestructuramos el taller de Lakeflow Spark Declarative Pipelines para:
1. Usar nombres de esquemas y tablas más limpios
2. Hacer de AUTO CDC el foco del Ejercicio 2
3. Usar un solo pipeline a lo largo de todo el taller (sin crear múltiples pipelines)
4. Implementar un flujo de mover archivos (no copiar/pegar)
5. Agregar programación para producción y mejores prácticas

## Cambios clave

### 1. Estructura de Catálogo y Esquemas

**Antes:**
- Catálogo: `sdp_workshop` (compartido)
- Esquemas: `1_bronze_db`, `2_silver_db`, `3_gold_db`

**Después:**
- Catálogo: `sdp_workshop_<username>` (por usuario)
- Esquemas: `bronze`, `silver`, `gold`
- Volumen: `raw` (en lugar de `workshop_data`)

### 2. Nomenclatura de Tablas

**Antes (redundante):**
- `bronze.orders_bronze`
- `silver.orders_silver`  
- `gold.gold_orders_by_date`

**Después (limpio):**
- `bronze.orders`
- `silver.orders_clean`
- `gold.order_summary`

### 3. Estructura de Carpetas

**Antes:**
```
/
├── 0-SETUP.py
├── 1-Building_Pipelines.sql
├── 2-Multi-Source.sql
└── transformations/
    ├── orders_pipeline.sql
    ├── status_pipeline.sql
    └── customers_pipeline.sql
```

**Después:**
```
/Setup/
  └── 0-SETUP.py
/Exercise_1/
  └── 1-Building_Pipelines_with_Data_Quality.sql
/Exercise_2/
  ├── 2-Production_CDC_and_Scheduling.sql
  └── customers_pipeline.sql (se mueve a /transformations durante el ejercicio)
/transformations/
  └── orders_pipeline.sql (usado en Exercise 1)
```

### 4. Reestructura del Ejercicio 2

**Antes:**
- Se creaba un segundo pipeline
- Se agregaban archivos de orders y status
- Se exploraban resultados (duplicado del Ejercicio 1)
- Luego se agregaba CDC de clientes como tercer archivo

**Después:**
- Usar el MISMO pipeline del Ejercicio 1
- Enfocarse inmediatamente en AUTO CDC
- Mover `customers_pipeline.sql` a transformations
- El pipeline auto‑descubre el nuevo archivo
- Profundizar en conceptos de CDC
- Programar para producción
- Mejores prácticas de producción

**Eliminado:**
- `status_pipeline.sql` (no necesario para el taller)
- Sección duplicada de “explorar resultados”
- Crear un segundo pipeline

**Agregado:**
- Explicación completa de AUTO CDC
- Comparación SCD Tipo 1 vs Tipo 2
- Detalle de cláusulas (KEYS, SEQUENCE BY, etc.)
- Guía de programación para producción
- Sección de monitoreo y mejores prácticas

### 5. Cambios en Setup

**Salida de Configuración:**
- Mensaje de impresión simplificado (sin caja decorativa)
- Esquema por defecto cambiado de `default` a `bronze`
- Nombre de volumen más limpio (`raw` en lugar de `workshop_data_<username>`)

**Creación de Catálogo:**
- Se agregó el paso de creación de catálogo (faltaba antes)

### 6. Actualizaciones de Archivos

#### 0-SETUP.py
- Agregado paso de creación de catálogo
- Actualizada la nomenclatura de esquemas
- Cambiado el nombre de volumen a `raw`
- Mensaje de finalización simplificado
- Funciones de UC funcionando correctamente

#### 1-Building_Pipelines_with_Data_Quality.sql
- Referencias de esquemas actualizadas
- Nombres de tablas corregidos en consultas
- Sección de resultados esperados actualizada
- Esquema por defecto cambiado a `bronze`

#### transformations/orders_pipeline.sql
- Ya estaba correcto con nombres limpios
- No se requieren cambios

#### Exercise_2/customers_pipeline.sql
- Todas las referencias de esquemas actualizadas:
  - `1_bronze_db.*` → `bronze.*`
  - `2_silver_db.*` → `silver.*`
  - `3_gold_db.*` → `gold.*`
- Nombres de tablas limpiados:
  - `customers_bronze_raw` → `customers_raw`
  - `customers_bronze_clean` → `customers_clean`
  - `customers_silver_scd1` → `customers`
  - `customer_summary_gold` → `customer_summary`

#### 2-Production_CDC_and_Scheduling.sql
- Notebook completamente nuevo
- Enfoque en AUTO CDC desde el inicio
- Explica conceptos de CDC a fondo
- Desglosa cada cláusula de AUTO CDC
- Agrega sección de programación para producción
- Incluye mejores prácticas

## Cambios en el Flujo de Trabajo

### Exercise 1 (flujo sin cambios)
1. Ejecutar setup
2. Crear pipeline
3. Configurar ajustes
4. Ejecutar pipeline
5. Explorar resultados

### Exercise 2 (flujo simplificado)
1. Revisar el código de `customers_pipeline.sql`
2. **Mover** el archivo a `transformations/`
3. El pipeline lo auto‑descubre
4. Generar datos CDC de ejemplo
5. Ejecutar pipeline con CDC
6. Probar actualizaciones CDC incrementales
7. Programar para producción
8. Aprender mejores prácticas

## Beneficios de los Cambios

1. **Nombres más limpios**: El esquema indica la capa; la tabla, la entidad  
2. **Un solo pipeline**: Sin confusiones creando múltiples pipelines; solo agrega archivos  
3. **Enfoque en CDC**: AUTO CDC es la lección principal  
4. **Listo para producción**: Incluye programación y mejores prácticas  
5. **Movimiento de archivos**: Refleja el flujo real de agregar fuentes  
6. **Integral**: Explicaciones profundas de conceptos CDC  
7. **Aislamiento por usuario**: Cada usuario tiene su propio catálogo

## Archivos Entregados

```
Lakeflow_SDP_Workshop.zip
├── README.md (resumen del taller e instrucciones)
├── Setup/
│   └── 0-SETUP.py (crea catálogo, esquemas, volumen, datos)
├── Exercise_1/
│   └── 1-Building_Pipelines_with_Data_Quality.sql
├── Exercise_2/
│   ├── 2-Production_CDC_and_Scheduling.sql
│   └── customers_pipeline.sql
└── transformations/
    └── orders_pipeline.sql
```

## Lista de Comprobación de Pruebas

- [ ] Setup crea el catálogo correctamente
- [ ] Setup crea esquemas (bronze, silver, gold)
- [ ] Setup crea el volumen raw
- [ ] Setup genera datos de ejemplo (orders y status)
- [ ] Exercise 1 crea el pipeline correctamente
- [ ] El pipeline de orders procesa correctamente
- [ ] El archivo de clientes del Exercise 2 se puede mover
- [ ] El pipeline auto‑descubre el nuevo archivo
- [ ] CDC procesa INSERT/UPDATE/DELETE correctamente
- [ ] Se puede configurar la programación
- [ ] Todas las consultas SQL corren sin errores

## Limitaciones Conocidas

1. **Funciones de UC**: Las funciones `add_orders`/`add_status` pueden tener problemas con `dbutils` en contexto UDF. Si fallan, los usuarios pueden generar datos con celdas de Python directamente.
2. **Pipeline de status**: Eliminado para enfocar en CDC. Puede agregarse en una versión de 3 horas.
3. **SCD Tipo 2**: Mencionado pero no implementado. Requeriría tiempo adicional.

## Mejoras Futuras

1. Agregar `status_pipeline.sql` al Exercise 2 para joins stream‑to‑stream  
2. Implementar ejemplo de SCD Tipo 2  
3. Agregar escenarios CDC más complejos (claves compuestas, deletes condicionales)  
4. Incluir ejercicios de optimización de rendimiento  
5. Agregar sección de pruebas y consultas de validación  

