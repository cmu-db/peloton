#!/bin/sh

# Set up environment
apt-get -y install git g++ pkg-config \
    valgrind libboost-dev libboost-filesystem-dev \
    libprotobuf-dev protobuf-compiler libevent-dev libjemalloc-dev libjsoncpp-dev \
    libgflags-dev libgoogle-glog-dev doxygen libsqlite3-dev
