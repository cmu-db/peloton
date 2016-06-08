#!/bin/sh

# Set up environment
apt-get --ignore-missing -y install \
    git g++ cmake \
    libprotobuf-dev protobuf-compiler libevent-dev \
    libboost-dev libboost-filesystem-dev \
    libjemalloc-dev libjsoncpp-dev valgrind \
    libunwind-dev libgflags-dev
