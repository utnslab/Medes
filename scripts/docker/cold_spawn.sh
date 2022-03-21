# Spawn for the first time, a container on the machine
# $1: Container ID of the container
# $2: Application to run

git clone https://github.com/DivyanshuSaxena/openwhisk-runtime-python
cd openwhisk-runtime-python
git checkout dedup

# Build image
sudo docker build -t actionloop-python-v3.7:1.0-SNAPSHOT $(pwd)/core/python3ActionLoop
cd ..

# Run container
PORT=$((1234+$1))
sudo docker run -d -p 127.0.0.1:${PORT}:8080/tcp --name=cont$1 -it actionloop-python-v3.7:1.0-SNAPSHOT

wget --post-file=scripts/appl/python-appl$2.json --header="Content-Type: application/json" http://localhost:${PORT}/init