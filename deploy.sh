#!/usr/bin/env bash
set -euo pipefail

cd /opt/airflow

echo "Configuring Docker authentication for Artifact Registry..."
gcloud auth configure-docker us-west1-docker.pkg.dev --quiet

echo "Pulling images and starting services..."
docker compose -f docker-compose.yml --env-file .env pull
docker compose -f docker-compose.yml --env-file .env up -d --remove-orphans

if [ ! -f "initialization_done" ]; then
    echo "Running Airflow database migration and user creation..."
    docker compose -f docker-compose.yml --env-file .env run --rm --no-deps airflow-init

if [ $? -eq 0 ]; then
        touch initialization_done
        echo "Airflow initialization succeeded."
    else
        echo "Airflow initialization FAILED. Check logs."
    fi
fi

echo "Pruning old images..."
docker image prune -f
