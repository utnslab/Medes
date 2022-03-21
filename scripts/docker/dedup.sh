# Checkpoint a running container and make it available for dedup.
# $1: Container ID of the container

# Stop the container as well -- does not occupy memory or cpu
# BROKEN: sudo docker checkpoint create --checkpoint-dir=/tmp/checkpoints/ cont$1 cont$1
mkdir -p /tmp/checkpoints/cont$1

# Correct execution outputs 'cp'
sudo docker checkpoint create cont$1 cp
# nohup sudo docker checkpoint create cont$1 cp &
# pid=$!

# # Add the pipe communication within bash
# echo -n cont$1 > /tmp/criu_fifo

# # Wait for completion
# wait $pid

# No need for a single directory
# mv /tmp/checkpoints/cp /tmp/checkpoints/cont$1

# ---------------- Start Restore and pause at a stage ----------------
# Start the checkpoint procedure in background (Use nohup to avoid process terminate)
nohup sudo docker start --checkpoint cp cont$1 &

# echo -n cont$1 > /tmp/criu_fifo

# Read the process ID from pipe
# read line < /tmp/criu_fifo
# echo $line