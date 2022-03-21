#!/usr/bin/env bash

cd $HOME

# all dependencies
sudo apt-get update
sudo apt-get install -y build-essential autoconf libtool pkg-config git libcurl4-gnutls-dev procps

# install cmake
wget https://github.com/Kitware/CMake/releases/download/v3.20.1/cmake-3.20.1.tar.gz
tar xzf cmake-3.20.1.tar.gz
pushd cmake-3.20.1
./bootstrap --parallel=$(getconf _NPROCESSORS_ONLN)
make -j$(getconf _NPROCESSORS_ONLN)
sudo make install
export PATH="/usr/local/bin:$PATH"
popd

# install grpc
git clone --recurse-submodules -b v1.35.0 https://github.com/grpc/grpc
pushd grpc
mkdir -p cmake/build
pushd cmake/build
cmake -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DCMAKE_INSTALL_PREFIX=$HOME/.local \
      ../..
make -j $(getconf _NPROCESSORS_ONLN)
make install
popd
popd

# install boost
wget https://boostorg.jfrog.io/artifactory/main/release/1.75.0/source/boost_1_75_0.tar.gz
tar xzf boost_1_75_0.tar.gz
pushd boost_1_75_0
sudo ./bootstrap.sh
sudo ./b2 -j$(getconf _NPROCESSORS_ONLN) install
popd

# install docker
./install-docker.sh

# install python pip
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3 get-pip.py
python3 -m pip install psutil

# install Mellanox OFED
wget http://www.mellanox.com/downloads/ofed/MLNX_OFED-5.0-2.1.8.0/MLNX_OFED_SRC-debian-5.0-2.1.8.0.tgz
tar xzf MLNX_OFED_SRC-debian-5.0-2.1.8.0.tgz
pushd MLNX_OFED_SRC-5.0-2.1.8.0/
sudo ./install.pl
popd

# install xdelta
wget https://github.com/jmacd/xdelta/archive/refs/tags/v3.1.0.zip
unzip v3.1.0.zip
pushd xdelta-3.1.0/xdelta3
autoreconf --install
./configure
make -j\$(getconf _NPROCESSORS_ONLN)
popd

# install openwhisk runtime fork and build the image
git clone https://github.com/DivyanshuSaxena/openwhisk-runtime-python.git
pushd openwhisk-runtime-python
git checkout dedup
sudo docker system prune --all --force
sudo docker build -t actionloop-python-v3.7:1.0-SNAPSHOT $(pwd)/core/python3ActionLoop
popd

# install Modified CRIU
sudo apt-get install -y gcc build-essential bsdmainutils python git-core asciidoc make htop git curl supervisor cgroup-lite libseccomp-dev libprotobuf-dev libprotobuf-c-dev protobuf-c-compiler protobuf-compiler python-protobuf libnl-3-dev libcap-dev libaio-dev libnet-dev
git clone https://github.com/DivyanshuSaxena/criu.git
pushd criu
git checkout mod-criu
sudo make -j8
sudo make install
popd

sudo reboot
