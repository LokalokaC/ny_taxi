#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Configuring Docker authentication for Artifact Registry..."
gcloud auth configure-docker us-west1-docker.pkg.dev --quiet

echo "Pulling images and starting services..."
if [ ! -f ".env" ]; then
  echo ".env not found in ${SCRIPT_DIR}"
  exit 1
fi

if [ ! -f "docker-compose.yml" ]; then
  echo "docker-compose.yml not found in ${SCRIPT_DIR}"
  exit 1
fi
docker compose -f docker-compose.yml --env-file .env pull
docker compose -f docker-compose.yml --env-file .env up -d --remove-orphans

if [ ! -f "initialization_done" ]; then
  echo "Running Airflow database migration and user creation..."
  if docker compose -f docker-compose.yml --env-file .env run --rm --no-deps airflow-init; then
    touch initialization_done
    echo "Airflow initialization succeeded."
  else
    echo "Airflow initialization FAILED. Check logs."
    exit 1
  fi
fi

echo "Pruning old images..."
docker image prune -f