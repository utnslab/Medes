# Checkpoint a running container but do not stop it.
# $1: Container ID of the container

mkdir -p /tmp/checkpoints/cont$1

# Usage:  docker checkpoint create [OPTIONS] CONTAINER CHECKPOINT (both arguments are cont$1)
# BROKEN: sudo docker checkpoint create --checkpoint-dir=/tmp/checkpoints/ --leave-running cont$1 cont$1
sudo docker checkpoint create --leave-running cont$1 cp
# nohup sudo docker checkpoint create --leave-running cont$1 cp &
# pid=$!

# # Add the pipe communication within bash
# echo -n cont$1 > /tmp/criu_fifo

# # Wait for completion
# wait $pid

# No need for a single directory
# mv /tmp/checkpoints/cp /tmp/checkpoints/cont$1
mv /tmp/checkpoints/*.img /tmp/checkpoints/cont$1/

# Freeze the container after checkpointing
sudo docker pause cont$1

# Remove all files in the dump except the pages file
sudo find /tmp/checkpoints/cont$1 -type f -not -name 'pages-*' -delete