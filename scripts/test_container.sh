# Spawn a new container on the machine
# $1: Container ID of the container

sudo docker inspect cont$1 | grep Error