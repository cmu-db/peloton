#!/bin/sh

# Set up environment
apt-get -y install git g++ autoconf pkg-config libtool libreadline-dev \
    libssl-dev valgrind libboost-dev libboost-filesystem-dev \
    python-pip python-xmlrunner \
    libprotobuf-dev protobuf-compiler libevent-dev libjemalloc-dev libjsoncpp-dev

# Pip
pip install unittest-xml-reporting
