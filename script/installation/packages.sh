#!/bin/sh

# Set up environment
apt-get -y install git g++ autoconf pkg-config libtool libreadline-dev \
    libssl-dev valgrind libboost-dev libboost-filesystem-dev \
    python-pip python-xmlrunner bison flex \
    libprotobuf-dev protobuf-compiler libevent-dev libjemalloc-dev libjsoncpp-dev \
    libgflags-dev libgoogle-glog-dev

# Pip
pip install unittest-xml-reporting
