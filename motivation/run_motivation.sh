#!/bin/bash

NUM_EXPS=10
CONT=0

e=0
while [ $e -lt $NUM_EXPS ]; do
    for i in 1 2; do
        # Spawn a new container
        PORT=$((1234 + $CONT))
        sudo docker run -d -p 127.0.0.1:${PORT}:8080/tcp --name=cont$CONT -it actionloop-python-v3.7:1.0-SNAPSHOT
        wget --server-response --post-file=../scripts/appl/python-appl$e.json --header="Content-Type: application/json" http://localhost:${PORT}/init

        # Get the container checkpoint
        ../scripts/docker/base.sh $CONT

        let CONT=$CONT+1
    done

    let e=$e+1
done
