# Spawn a new container on the machine
# $1: Container ID of the container
# $2: Application to run

PORT=$((1234+$1))

sudo docker run -d -p 127.0.0.1:${PORT}:8080/tcp --name=cont$1 -it actionloop-python-v3.7:1.0-SNAPSHOT

wget --server-response --post-file=scripts/appl/python-appl$2.json --header="Content-Type: application/json" http://localhost:${PORT}/init 2>&1 | awk '/^  HTTP/{print $2}'
