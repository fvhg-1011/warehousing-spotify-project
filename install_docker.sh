#!/bin/bash

set -e

echo "Installing Docker and Docker Compose..."

# Update and upgrade the system
echo "Updating system packages..."
sudo apt update -y
sudo apt upgrade -y

# Install Docker dependencies
echo "Installing dependencies..."
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common

# Add Docker GPG key
echo "Adding Docker GPG key..."
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

# Add Docker repository
echo "Adding Docker repository..."
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo apt update -y

# Install Docker CE
echo "Installing Docker..."
sudo apt install -y docker-ce

# Add the current user to the Docker group
echo "Adding user $USER to the Docker group..."
sudo usermod -aG docker $USER

# Change ownership of Docker socket
echo "Changing ownership of Docker socket..."
sudo chown $USER:docker /var/run/docker.sock

# Restart Docker service
echo "Restarting Docker service..."
sudo systemctl restart docker

# Install Docker Compose manually
echo "Installing Docker Compose..."
DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}
mkdir -p $DOCKER_CONFIG/cli-plugins
curl -SL https://github.com/docker/compose/releases/download/v2.27.0/docker-compose-linux-x86_64 -o $DOCKER_CONFIG/cli-plugins/docker-compose
sudo chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose

# Verify Docker Compose installation
echo "Verifying Docker Compose installation..."
docker compose version

echo "Docker and Docker Compose installation completed successfully."
