#!/bin/sh

# Set up environment
apt-get -y install git g++ autoconf pkg-config libtool libreadline-dev \
    libssl-dev valgrind libboost1.55-dev libboost1.55-all-dev \
    python-pip python-xmlrunner \
    libprotobuf-dev protobuf-compiler libevent-dev libjemalloc-dev

# Pip
pip install unittest-xml-reporting
