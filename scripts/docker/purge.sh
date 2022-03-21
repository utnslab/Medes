# Purge an existing container on the machine
# $1: Container ID of the container

# Remove any checkpoints and associated directories of the container
sudo docker checkpoint rm cont$1 cp
sudo rm -rf /tmp/checkpoints/cont$1

sudo docker stop cont$1
sudo docker rm cont$1