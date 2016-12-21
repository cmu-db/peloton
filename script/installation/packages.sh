#!/bin/bash

# Determine OS platform
UNAME=$(uname | tr "[:upper:]" "[:lower:]")
# If Linux, try to determine specific distribution
if [ "$UNAME" == "linux" ]; then
    # If available, use LSB to identify distribution
    if [ -f /etc/lsb-release -o -d /etc/lsb-release.d ]; then
        export DISTRO=$(lsb_release -i | cut -d: -f2 | sed s/'^\t'//)
    # Otherwise, use release info file
    else
        export DISTRO=$(ls -d /etc/[A-Za-z]*[_-][rv]e[lr]* | grep -v "lsb" | cut -d'/' -f3 | cut -d'-' -f1 | cut -d'_' -f1)
    fi
fi
# For everything else (or if above failed), just use generic identifier
[ "$DISTRO" == "" ] && export DISTRO=$UNAME
unset UNAME
DISTRO=${DISTRO^^}

## ------------------------------------------------
## UBUNTU
## ------------------------------------------------
if [ "$DISTRO" = "UBUNTU" ]; then
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
## ------------------------------------------------
## FEDORA/REDHAT
## ------------------------------------------------
elif [[ "$DISTRO" == *"FEDORA"* ]]; then
    dnf install -y git \
        gcc-c++ \
        cmake \
        gflags-devel \
        protobuf-devel \
        bison \
        flex \
        libevent-devel \
        boost-devel \
        jemalloc-devel \
        valgrind \
        lcov \
        postgresql
## ------------------------------------------------
## UNKNOWN
## ------------------------------------------------
else
    echo "Unknown distribution '$DISTRO'"
    echo "Please contact our support team for additional help." \
         "Be sure to include the contents of this message"
    echo "Platform: $(uname -a)"
    echo
    echo "https://github.com/cmu-db/peloton/issues"
    echo
    exit 1
fi