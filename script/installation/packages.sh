#!/bin/bash -x

# Determine OS platform
UNAME=$(uname | tr "[:upper:]" "[:lower:]")
# If Linux, try to determine specific distribution
if [ "$UNAME" == "linux" ]; then
    # If available, use LSB to identify distribution
    if [ -f /etc/lsb-release -o -d /etc/lsb-release.d ]; then
        export DISTRO=$(lsb_release -is)
        DISTRO_VER=$(lsb_release -rs)
    # Otherwise, use release info file
    else
        export DISTRO=$(ls -d /etc/[A-Za-z]*[_-][rv]e[lr]* | grep -v "lsb" | cut -d'/' -f3 | cut -d'-' -f1 | cut -d'_' -f1)
        DISTRO_VER=$(cat /etc/*-release | grep "VERSION_ID" | cut -d'=' -f2 | tr -d '"')
    fi
fi
# For everything else (or if above failed), just use generic identifier
[ "$DISTRO" == "" ] && export DISTRO=$UNAME
unset UNAME
DISTRO=$(echo $DISTRO | tr "[:lower:]" "[:upper:]")
TMPDIR=/tmp

## ------------------------------------------------
## UBUNTU
## ------------------------------------------------
if [ "$DISTRO" = "UBUNTU" ]; then
    sudo apt-get --force-yes --ignore-missing -y install \
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
        libpqxx-dev \
        llvm-3.9 \
        libedit-dev \
        postgresql-client

## ------------------------------------------------
## FEDORA
## ------------------------------------------------
elif [[ "$DISTRO" == *"FEDORA"* ]]; then
    sudo dnf install -y \
        git \
        gcc-c++ \
        cmake \
        gflags-devel \
        protobuf-devel \
        bison \
        flex \
        libevent-devel \
        openssl-devel \
        boost-devel \
        jemalloc-devel \
        valgrind \
        lcov \
        libpqxx-devel \
        libpqxx \
        llvm \
        llvm-devel \
        llvm-static \
        libedit-devel \
        postgresql

## ------------------------------------------------
## REDHAT
## ------------------------------------------------
elif [[ "$DISTRO" == *"REDHAT"* ]] && [[ "${DISTRO_VER%.*}" == "7" ]]; then
    # Add EPEL repository first
    sudo yum install -y epel-release

    # Simple installations via yum
    sudo yum install -y \
        git \
        gcc-c++ \
        cmake \
        make \
        gflags-devel \
        protobuf-devel \
        bison \
        flex \
        libevent-devel \
        openssl-devel \
        boost-devel \
        jemalloc-devel \
        valgrind \
        lcov \
        m4 \
        doxygen \
        graphviz \
        libpqxx-devel \
        libpqxx \
        llvm3.9 \
        llvm3.9-devel \
        llvm3.9-static \
        libedit-devel \
        postgresql

## ------------------------------------------------
## DARWIN (OSX)
## ------------------------------------------------
elif [ "$DISTRO" = "DARWIN" ]; then
    if test ! $(which brew); then
      echo "Installing homebrew..."
      ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    fi

    brew install git
    brew install cmake
    brew install gflags
    brew install protobuf
    brew install bison
    brew install flex
    brew install libevent
    brew install boost
    brew install jemalloc
    brew install lcov
    brew install libpqxx
    brew install libedit
    brew install llvm@3.7
    brew install postgresql

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
