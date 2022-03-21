#!/bin/bash
# Script to setup a Python environment on LXC containers
# Arguments:
# 1: Library to install

# Set up the apt repo
sudo apt update
sudo apt install -y curl software-properties-common

# Install python
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install -y python3.8 python3.8-distutils

# Install PIP
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3.8 get-pip.py

# Install specific package
python3.8 -m pip install $1
