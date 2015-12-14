#!/bin/bash

BASEDIR=$(dirname $0)
cd $BASEDIR

# GO TO NVML
cd ../../third_party/nvml

if test -e "Makefile";then
    # BUILD
    make -j4

    # INSTALL
    sudo make install
fi

