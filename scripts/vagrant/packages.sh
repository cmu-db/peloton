#!/bin/sh

# Set up environment
apt-get -y install git g++ autoconf pkg-config libtool libjson-spirit-dev libreadline-dev \
    libmm-dev libdw-dev libssl-dev python-numpy uuid-dev clang-format-3.3 valgrind \
    python-pip python-xmlrunner default-jdk default-jre ant kcachegrind libboost-all-dev

# Pip
pip install --upgrade unittest-xml-reporting

# Get dependencies script
wget https://raw.githubusercontent.com/cmu-db/peloton/master/scripts/installation/dependencies.py

# Install dependencies
python dependencies.py

# Cleanup
rm dependencies.py

# Setup log dir
mkdir -p /mnt/pmfs
chmod 777 /mnt/pmfs
