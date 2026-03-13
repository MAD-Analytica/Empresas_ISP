# Fuentes por pais y esquema canonico

Este documento define, para la ventana 2024-2025, las fuentes oficiales de accesos por pais y el esquema canonico de consolidacion para el estudio ICP (1000 a 100000 usuarios).

## Regla comun del estudio

- Ventana temporal: anos calendario 2024 y 2025.
- Criterio ICP: `1000 <= max_accesos_2024_2025 <= 100000`.
- Unidad de analisis para ICP: operador por pais.

## Fuentes de accesos por pais

### Colombia (COL)

- Autoridad/fuente: postdata.gov.co (DKAN Datastore) con recurso ya usado en el proyecto.
- Tipo de extraccion: API (paginada por `limit/offset`).
- Script esperado: `scripts/extract_colombia.py` (o mantener `scripts/extract.py` como extractor COL).
- Estado: automatizable con el flujo actual.

### Ecuador (ECU)

- Autoridad/fuente: ARCOTEL, seccion "Abonados y usuarios".
- URL base: `https://www.arcotel.gob.ec/abonados-y-usuarios/`.
- Tipo de extraccion: descarga de Excel por trimestre (manual o semiautomatizada).
- Script esperado: `scripts/extract_ecuador.py`.
- Nota operativa: si no hay API estable ni catalogo de datos abierto utilizable, se toma el XLSX oficial trimestral como fuente primaria.

### Peru (PER)

- Autoridad/fuente principal: OSIPTEL (Datos Abiertos Peru).
- URL candidata: `https://www.datosabiertos.gob.pe/dataset/cantidad-de-conexiones-en-servicio-de-internet-fijo-por-velocidad-de-bajada-empresa`.
- Tipo de extraccion: descarga de CSV/XLSX publicado en el dataset.
- Script esperado: `scripts/extract_peru.py`.
- Nota operativa: no depender de una API tabular tipo DKAN search; priorizar recursos descargables versionados por el portal.

## Esquema canonico minimo

Todos los extractores deben converger a estas columnas:

- `pais`: codigo ISO corto del pais (`COL`, `ECU`, `PER`).
- `id_operador`: identificador estable del operador en la fuente (si no existe, usar normalizacion de nombre y marcar origen).
- `operador`: nombre del operador.
- `anno`: ano numerico.
- `trimestre`: entero 1..4 (si la fuente es mensual, mapear mes a trimestre).
- `num_accesos`: accesos/conexiones en servicio para el periodo.
- `fuente`: identificador de la fuente (`postdata_dkan`, `arcotel_xlsx`, `osiptel_open_data`).

## Campos recomendados (no obligatorios)

- `segmento`
- `tecnologia`
- `departamento_provincia`
- `rango_velocidad`
- `fecha_corte`
- `url_origen`
- `archivo_origen`

## Reglas de estandarizacion

- `operador`: trim, uppercase, colapsar espacios.
- `num_accesos`: entero no negativo.
- `anno`: entre 2024 y 2025 para calculo ICP.
- `trimestre`: 1..4.
- Sin duplicados por llave (`pais`, `id_operador`, `anno`, `trimestre`) luego de agregacion.

## Validaciones minimas al cargar cada pais

- Cobertura de periodos esperados en 2024-2025.
- Porcentaje de registros con `id_operador` nulo.
- Top 10 operadores por accesos contra publicacion oficial (control de razonabilidad).
- Total de accesos por trimestre no decrece por errores de parseo de formato numerico.
