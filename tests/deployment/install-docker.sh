#!/usr/bin/env bash
set -euo pipefail

# Install prerequisite system packages needed before Docker can be added.
# python3-venv and git are required later by the cosiflowctl deployment tool.
sudo apt-get update
sudo apt-get install -y ca-certificates curl python3-venv git

# Create the keyring directory where Docker's GPG signing key will be stored.
sudo install -m 0755 -d /etc/apt/keyrings

# Download Docker's official GPG key and store it in the system keyring.
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add Docker's APT repository for the current Ubuntu codename (e.g. "resolute" for 26.04).
# Using the deb822 Sources format rather than the legacy one-liner format.
sudo tee /etc/apt/sources.list.d/docker.sources >/dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

# Refresh the package index to pick up the new Docker repository.
sudo apt-get update

# Install Docker CE plus the Compose plugin (v2, integrated into the docker CLI).
# docker-buildx-plugin is required by cosiflowctl's image build step.
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Start the Docker daemon immediately without waiting for a reboot.
sudo systemctl start docker

# Confirm the daemon and Compose plugin are both functional.
sudo docker version
sudo docker compose version

# Allow the ubuntu user to run docker without sudo.
# NOTE: this change takes effect only in a NEW SSH session, not the current one.
# After this script finishes, re-open the SSH connection before running any docker command.
sudo usermod -aG docker "$USER"
