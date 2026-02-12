# Empresas ISP Colombia

Pipeline ETL para extraer datos de prestadores de internet fijo en Colombia (CRC) y enriquecerlos con información de representantes legales (RUES) y contactos técnicos (WHOIS).

## Setup

```bash
pip install -r requirements.txt
```

## Uso

**Pipeline completo** (extracción + transformación + enriquecimiento):
```bash
python main.py
```

**Solo enriquecimiento** (usa datos ya extraídos):
```bash
python scripts/enrich.py
```

**Separar en tablas empresas/leads**:
```bash
python scripts/split_tables.py
```

## Estructura de salida

- `data/processed/empresas_enriquecidas-{fecha}.csv` - Empresas con datos de RUES y WHOIS
- `data/processed/finals/tabla-empresas-{fecha}.csv` - Info corporativa
- `data/processed/finals/tabla-leads-{fecha}.csv` - Contactos (Rep. Legal, Responsable Técnico, Contacto Técnico)

## Configuración

Editar `config.py` para cambiar:
- `RESOURCE_ID` - ID del dataset en PostData
- `LOAD_DATA_DIR` - Carpeta destino en Drive
