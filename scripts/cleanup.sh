# Cleanup all docker containers and checkpoints, for the next run
pgrep criu | xargs kill -9
sudo rm /tmp/criu_fifo
sudo rm /tmp/restore_fifo

sudo docker stop $(sudo docker ps -a -q)
sudo docker rm $(sudo docker ps -a -q)

rm -rf /tmp/checkpoints
mkdir -p /tmp/checkpoints
