#!/bin/sh

# Set up environment
apt-get --ignore-missing -y install \
    git \
    g++ \
    cmake \
    libgflags-dev \
    libprotobuf-dev \
    protobuf-compiler \
    bison \
    flex \
    libevent-dev \
    libboost-dev \
    libboost-thread-dev \
    libboost-filesystem-dev \
    libjemalloc-dev \
    valgrind \
    lcov \
    postgresql-client
