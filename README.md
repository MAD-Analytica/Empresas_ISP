# Empresas ISP Multicountry

Pipeline para analizar ISPs en Colombia, Ecuador y Peru con criterio ICP (`1000 <= max_accesos_2024_2025 <= 100000`), enriquecer con WHOIS y generar tablas finales.

## Flujo recomendado

1. Colocar archivos fuente por pais:
   - `data_ISPs/raw/colombia/*.csv`
   - `data_ISPs/raw/ecuador/*.xlsx`
   - `data_ISPs/raw/peru/*.{csv,xlsx}`
2. Ejecutar pipeline completo:

```bash
python3 main.py
```

3. Salidas principales:
   - `data_ISPs/processed/icp_operadores_2024_2025.csv`
   - `data_ISPs/processed/icp_resumen_pais_2024_2025.csv`
   - `data_ISPs/processed/icp_operadores_whois_2024_2025.csv`
   - `data_ISPs/processed/finals/tabla-empresas-icp-whois.csv`
   - `data_ISPs/processed/finals/tabla-leads-icp-whois.csv`

## Dashboard

```bash
streamlit run dashboard_isp.py
```

Permite filtrar por pais, rango de usuarios y nombre de empresa; incluye KPIs, charts y tabla de ISPs.

## Deploy en Cloud Run (datos embebidos)

Este repo incluye `Dockerfile`, `.dockerignore` y `.gcloudignore` para desplegar el dashboard con los CSV de `data_ISPs/processed` dentro de la imagen.

### 1) Configura proyecto/región

```bash
gcloud config set project TU_PROJECT_ID
gcloud config set run/region us-central1
```

### 2) Build y deploy

```bash
gcloud builds submit --tag gcr.io/TU_PROJECT_ID/empresas-isp-dashboard

gcloud run deploy empresas-isp-dashboard \
  --image gcr.io/TU_PROJECT_ID/empresas-isp-dashboard \
  --platform managed \
  --allow-unauthenticated \
  --port 8080 \
  --memory 1Gi
```

### 3) Abrir servicio

```bash
gcloud run services describe empresas-isp-dashboard --format='value(status.url)'
```
