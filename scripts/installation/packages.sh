#!/bin/sh

# Set up environment
apt-get -y install git g++ autoconf pkg-config libtool libjson-spirit-dev libreadline-dev \
    libssl-dev valgrind \
    python-pip python-xmlrunner \
    libprotobuf-dev protobuf-compiler libevent-dev libjemalloc-dev

# Pip
pip install unittest-xml-reporting

# Install boost 1.55
add-apt-repository ppa:boost-latest/ppa
apt-get -y update
apt-get install -y libboost1.55-all-dev
