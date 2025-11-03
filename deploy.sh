#!/usr/bin/env bash
set -euo pipefail

cd /opt/airflow

if [ ! -f "initialization_done" ]; then
    echo "Running Airflow initialization..."
    docker compose -f docker-compose.yml --env-file .env run --rm --no-deps airflow-init
    touch initialization_done
fi

echo "Pulling latest image..."
docker compose -f docker-compose.yml --env-file .env pull

echo "Starting services..."
docker compose -f docker-compose.yml --env-file .env up -d --remove-orphans

echo "Pruning old images..."
docker image prune -f
