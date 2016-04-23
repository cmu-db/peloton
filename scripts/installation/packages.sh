#!/bin/sh

# Set up environment
apt-get -y install git g++ autoconf pkg-config libtool libjson-spirit-dev libreadline-dev \
    libssl-dev valgrind \
    python-pip python-xmlrunner libboost1.55-dev libboost1.55-all-dev \
    libprotobuf-dev protobuf-compiler libevent-dev libjemalloc-dev

# Pip
pip install unittest-xml-reporting
