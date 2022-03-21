#!/usr/bin/env bash

mkdir -p cmake/build
pushd cmake/build
cmake -DCMAKE_PREFIX_PATH=$HOME/.local/ -DCMAKE_BUILD_TYPE=Debug -DXDELTA_DIR=$HOME/xdelta-3.1.0/xdelta3 ../..
make -j$(getconf _NPROCESSORS_ONLN)
popd
