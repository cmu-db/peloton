#!/bin/sh

# Set up environment
apt-get -y install git g++ cmake \
    libboost-dev libboost-filesystem-dev \
    libprotobuf-dev protobuf-compiler libevent-dev libjemalloc-dev libjsoncpp-dev \
    doxygen valgrind libsqlite3-dev
