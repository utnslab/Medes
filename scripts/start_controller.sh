# 1: Testcase to run requests from

cd $HOME
sudo rm logfileC

# Update the testcase file
cp dedup/evaluation/testcases/testcase$1 dedup/config/requests

# Start the Macro Stats
python3 dedup/evaluation/macro_stats.py cont &
pid=$!

# Start the controller
pushd dedup/cmake/build
./dedup-controller/controller 20 ../../config/requests |& tee ~/logfileC
# sudo gdb ./dedup-controller/controller -q -ex "catch throw std::out_of_range" -ex "r 20 ../../config/requests" -ex bt -ex "continue" |& sudo tee ~/logfileC
popd

# Stop the Macro Stats
sudo kill -SIGINT $pid

exec $SHELL