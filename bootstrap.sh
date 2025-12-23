#! /usr/bin/env bash
set -euxo pipefail
exec > >(tee -a /var/log/startup-script.log) 2>&1
export DEBIAN_FRONTEND=noninteractive

DEPLOY_USER="github-actions"
VM_DEPLOY_DIR="/opt/airflow"
GAR_REGION="us-west1"

echo "[startup] Begin setup at $(date -Is)"

# 0) Basic config
apt-get update -y
apt-get install -y --no-install-recommends ca-certificates curl gnupg lsb-release apt-transport-https

# 1. Install Docker
if ! command -v docker >/dev/null 2>&1; then
  echo "[startup] Installing Docker (official repo)..."
  install -m 0755 -d /etc/apt/keyrings
  if [[ ! -f /etc/apt/keyrings/docker.gpg ]]; then
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
  fi
  UBUNTU_CODENAME="$(. /etc/os-release && echo "$VERSION_CODENAME")"  # noble
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${UBUNTU_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  systemctl enable --now docker
else
  echo "[startup] Docker already present; ensuring service is running"
  systemctl enable --now docker
fi

# 2. Set permissions for using Docker
if id "${DEPLOY_USER}" >/dev/null 2>&1; then
  echo "[startup] User ${DEPLOY_USER} already exists."
else
  echo "[startup] Creating user ${DEPLOY_USER}"
  useradd -m -s /bin/bash "${DEPLOY_USER}"
fi

if id -nG "${DEPLOY_USER}" | grep -qw docker; then
  echo "[startup] ${DEPLOY_USER} already in docker group."
else
  echo "[startup] Adding ${DEPLOY_USER} to docker group"
  usermod -aG docker "${DEPLOY_USER}" || true
fi

# 3. Prepare deploy directory with proper ownership
echo "[startup] Preparing deploy dir ${VM_DEPLOY_DIR}"
mkdir -p "${VM_DEPLOY_DIR}"
chown -R "${DEPLOY_USER}:${DEPLOY_USER}" "${VM_DEPLOY_DIR}"

# 4. Install gcloud CLI
if ! command -v gcloud >/dev/null 2>&1; then
  echo "[startup] Installing gcloud CLI..."
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
    > /etc/apt/sources.list.d/google-cloud-sdk.list
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
  apt-get update -y && apt-get install -y google-cloud-cli
fi

# 5. Set authorization for Artifact Registry
echo "[startup] Configuring Artifact Registry Docker credential helper for ${GAR_REGION}"
gcloud auth configure-docker "${GAR_REGION}-docker.pkg.dev" --quiet || true
docker pull us-west1-docker.pkg.dev/spartan-lacing-477506-k1/github-actions/ny_taxi:latest || true


# 6. Optional: verify docker works
docker --version || true
docker compose version || true
gcloud --version || true

echo "[startup] Completed setup at $(date -Is)"
