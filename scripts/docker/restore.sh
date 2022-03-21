# Restore a dedup container from a checkpoint.
# Assumption: Memory Manager has already fixed the deduped dump
# into CRIU dump at the requisite location in tmpfs
# $1: PID of Restore Process

# No need for a single directory
# mv /tmp/checkpoints/cont$1 /tmp/checkpoints/cp

# Send a signal to the CRIU restore process
sudo kill -SIGUSR1 $1

# Wait for completion from CRIU
# read line < /tmp/restore_fifo