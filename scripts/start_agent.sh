cd $HOME

# Switch ASLR off
echo 0 | sudo tee /proc/sys/kernel/randomize_va_space

# Cleanup previous experiment first
sudo ./dedup/scripts/cleanup.sh

# Restart docker and containerd-shim processes
pgrep dockerd | sudo xargs kill
pgrep containerd-shim | sudo xargs kill
sleep 2

sudo bash ./dedup/scripts/start_docker.sh > start_docker.log

# Start the Macro Stats
sudo python3 dedup/evaluation/macro_stats.py agent$1 > python.log 2>&1 &

# Start the agent
pushd dedup/cmake/build
sudo rm init*
sudo ./dedup-service/dedup_appl $1 20 |& sudo tee ~/logfile$1
# sudo gdb ./dedup-service/dedup_appl -q -ex "catch throw std::out_of_range" -ex "r $1 20" -ex bt |& sudo tee ~/logfile$1
# sleep 1
# sudo memleak-bpfcc -p $(pidof dedup_appl) -o 120000 --combined-only -T 20 15 120 > ~/memcheck$1
popd

# Stop the Macro Stats
pid=$(pgrep -f macro)
sudo kill -SIGINT $pid
# wait $(pidof dedup_appl)

exec $SHELL