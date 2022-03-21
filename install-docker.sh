#!/bin/bash
# Script to setup Docker for container checkpoints on a machine

wget https://download.docker.com/linux/static/stable/x86_64/docker-19.03.12.tgz
tar -xvf docker-19.03.12.tgz

# Remove previous versions of docker, containerd, runc
sudo rm /usr/bin/containerd*
sudo rm /usr/bin/docker*
sudo rm /usr/bin/runc*
sudo rm /usr/bin/ctr*

# Copy executables onto a path
sudo cp docker/* /usr/bin/

# Start Docker Daemon
sudo dockerd > /dev/null 2>&1 &
sleep 1

# Stop daemon and enable experimental features
pgrep dockerd | sudo xargs kill
pgrep containerd-shim | sudo xargs kill
sleep 2

# Start docker in experimental mode
sudo rm -rf /var/lib/docker/
echo "{\"experimental\": true}" | sudo tee /etc/docker/daemon.json
sleep 1

sudo dockerd > /dev/null 2>&1 &
sleep 1
