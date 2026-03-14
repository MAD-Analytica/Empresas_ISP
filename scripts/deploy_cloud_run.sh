#!/usr/bin/env bash
set -euo pipefail

# Uso:
#   PROJECT_ID=tu-project REGION=us-central1 SERVICE_NAME=empresas-isp-dashboard ./scripts/deploy_cloud_run.sh

PROJECT_ID="${PROJECT_ID:-}"
REGION="${REGION:-us-central1}"
SERVICE_NAME="${SERVICE_NAME:-empresas-isp-dashboard}"
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

if [[ -z "${PROJECT_ID}" ]]; then
  echo "ERROR: define PROJECT_ID. Ejemplo:"
  echo "  PROJECT_ID=mi-proyecto REGION=us-central1 SERVICE_NAME=empresas-isp-dashboard ./scripts/deploy_cloud_run.sh"
  exit 1
fi

echo ">> Proyecto: ${PROJECT_ID}"
echo ">> Region:   ${REGION}"
echo ">> Servicio: ${SERVICE_NAME}"
echo ">> Imagen:   ${IMAGE}"

gcloud config set project "${PROJECT_ID}"
gcloud config set run/region "${REGION}"

echo ">> Build de imagen..."
gcloud builds submit --tag "${IMAGE}"

echo ">> Deploy en Cloud Run..."
gcloud run deploy "${SERVICE_NAME}" \
  --image "${IMAGE}" \
  --platform managed \
  --allow-unauthenticated \
  --port 8080 \
  --memory 1Gi

echo ">> URL del servicio:"
gcloud run services describe "${SERVICE_NAME}" --format='value(status.url)'

