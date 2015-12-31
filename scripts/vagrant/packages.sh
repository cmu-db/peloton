#!/bin/sh

# Set up environment
apt-get -y install git g++ autoconf pkg-config libtool libjson-spirit-dev libreadline-dev libmm-dev libdw-dev libssl-dev

# Get dependencies script
wget https://raw.githubusercontent.com/cmu-db/peloton/master/scripts/installation/dependencies.py

# Install dependencies
python dependencies.py

# Cleanup
rm dependencies.py
