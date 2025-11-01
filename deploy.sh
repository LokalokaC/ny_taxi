#!/usr/bin/env bash
set -euo pipefail

cd /opt/airflow

docker compose -f docker-compose.prod.yml --env-file .env pull
docker compose -f docker-compose.prod.yml --env-file .env up -d --remove-orphans

# 可選：清掉舊 image
docker image prune -f