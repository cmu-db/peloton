#!/bin/sh

# Set up environment
apt-get -y install git g++ autoconf pkg-config libtool libjson-spirit-dev libreadline-dev \
    libssl-dev valgrind \
    python-pip python-xmlrunner \
    libprotobuf-dev protobuf-compiler libevent-dev libjemalloc-dev

# Pip
pip install unittest-xml-reporting

# Install latest boost (1.55)
add-apt-repository -y ppa:boost-latest/ppa
apt-get -y --purge remove libboost-all-dev libboost-dev libboost-doc
apt-get -y install -f
dpkg --configure -a
apt-get -y update
apt-get install -y libboost1.55-tools-dev libboost1.55-dev libboost1.55-all-dev
