#!/usr/bin/env bash
set -euo pipefail

DEPLOY_USER="github-actions"
VM_DEPLOY_DIR="/opt/airflow" 

echo "Starting VM setup via startup-script..." >> /var/log/startup-script.log

# 1. Install Docker
echo "Installing Docker..." >> /var/log/startup-script.log
apt-get update
apt-get install -y ca-certificates curl gnupg lsb-release
mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# 2. Set permissions for using Docker
echo "Setting up Docker permissions for $DEPLOY_USER" >> /var/log/startup-script.log
usermod -aG docker $DEPLOY_USER

# 3. Grant permissions for DIR
echo "Setting up deploy directory $VM_DEPLOY_DIR" >> /var/log/startup-script.log
mkdir -p $VM_DEPLOY_DIR
chown -R $DEPLOY_USER:$DEPLOY_USER $VM_DEPLOY_DIR

echo "Bootstrap complete. Environment is ready." >> /var/log/startup-script.log
