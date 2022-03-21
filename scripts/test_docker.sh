#!/bin/bash
# Script to test if container checkpoint restore is working with Docker

sudo docker run -d --name looper busybox /bin/sh -c 'i=0; while true; do echo $i; i=$(expr $i + 1); sleep 1; done'
sudo docker checkpoint create looper checkpoint1
sudo docker start --checkpoint checkpoint1 looper
sudo docker logs looper



sudo docker stop cont1
sudo docker start cont1
sudo mkdir -p /tmp/checkpoints/cp

echo "STARTING CHECKPOINT"
time sudo docker checkpoint create cont1 cp
# time sudo docker checkpoint create --checkpoint-dir=/tmp/checkpoints/ cont1 cp
cat /tmp/custom-main-logs.txt
sudo ls /tmp/checkpoints/cp

echo "======================================="
echo "STARTING RESTORE"
time sudo docker start --checkpoint cp cont1
cat /tmp/custom-main-logs.txt

echo "======================================="
echo "CLEANUP"
sudo docker checkpoint rm cont1 cp
sudo rm /tmp/custom-main-logs.txt
sudo rm -rf /tmp/checkpoints/cp