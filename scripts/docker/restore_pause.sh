# Start the restore process of a container, which pauses at a stage and gives a return PID
# $1: Container ID of the container

# First stop the container, so that it can be restored
sudo docker stop cont$1

# ---------------- Start Restore and pause at a stage ----------------
# Start the checkpoint procedure in background (Use nohup to avoid process terminate)
nohup sudo docker start --checkpoint cp cont$1 &

# echo -n cont$1 > /tmp/criu_fifo

# Read the process ID from pipe
# read line < /tmp/criu_fifo
# echo $line