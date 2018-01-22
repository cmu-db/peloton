#!/bin/bash -x

## =================================================================
## PELOTON PACKAGE INSTALLATION
##
## This script will install all the packages that are needed to
## build and run the DBMS.
##
## Note: On newer versions of Ubuntu (17.04), this script
## will not install the correct version of g++. You will have
## to use 'update-alternatives' to configure the default of
## g++ manually.
##
## Supported environments:
##  * Ubuntu (14.04, 16.04)
##  * Fedora
##  * OSX
## =================================================================

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
    # Fix for LLVM-3.7 on Ubuntu 14.04
    if [ "$DISTRO_VER" == "14.04" ]; then
        if ! grep -q 'deb http://llvm.org/apt/trusty/ llvm-toolchain-trusty-3.7 main' /etc/apt/sources.list; then
            echo 'deb http://llvm.org/apt/trusty/ llvm-toolchain-trusty-3.7 main' | sudo tee -a /etc/apt/sources.list > /dev/null
        fi
        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 15CF4D18AF4F7421
        sudo apt-get update -qq
    fi

    # Fix for cmake3 name change in Ubuntu 15.x and 16.x plus --force-yes deprecation
    CMAKE_NAME="cmake3"
    FORCE_Y="--force-yes"
    MAJOR_VER=$(echo "$DISTRO_VER" | cut -d '.' -f 1)
    for version in "15" "16"
    do
       if [ "$MAJOR_VER" = "$version" ]
       then
          FORCE_Y=""
          CMAKE_NAME="cmake"
          break
       fi
    done

    sudo apt-get -qq $FORCE_Y --ignore-missing -y install \
        git \
        g++ \
        clang-3.7 \
        $CMAKE_NAME \
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
        llvm-3.7 \
        libedit-dev \
        postgresql-client

## ------------------------------------------------
## FEDORA
## ------------------------------------------------
elif [[ "$DISTRO" == *"FEDORA"* ]]; then
    sudo dnf -q install -y \
        git \
        gcc-c++ \
        make \
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
        postgresql \
        libatomic

## ------------------------------------------------
## REDHAT
## ------------------------------------------------
elif [[ "$DISTRO" == *"REDHAT"* ]] && [[ "${DISTRO_VER%.*}" == "7" ]]; then
    function install_package() {
        if [ "$#" -lt 1 ]; then
            echo "The download path is required."
            exit 1
        fi

        pushd $TMPDIR
        wget -nc --no-check-certificate "$1"
        tpath=$(basename "$1")
        dpath=$(tar --exclude='*/*' -tf "$tpath")
        tar xzf $tpath
        pushd $dpath
        if [ -e "bootstrap.sh" ]; then
            ./bootstrap.sh
            sudo ./b2 install
        else
            ./configure
            make
            sudo make install
        fi
        popd; popd
        return 0
    }

    # Package download paths
    PKGS=(
        "https://github.com/schuhschuh/gflags/archive/v2.0.tar.gz"
    )

    # Add EPEL repository first
    sudo yum -q -y install epel-release
    sudo yum -q -y upgrade epel-release

    # Simple installations via yum
    sudo yum -q -y install \
        git \
        gcc-c++ \
        make \
        cmake \
        flex \
        bison \
        libevent-devel \
        openssl-devel \
        boost-devel \
        protobuf-devel \
        jemalloc-devel \
        libedit-devel \
        valgrind \
        lcov \
        m4 \
        doxygen \
        graphviz \
        libpqxx \
        libpqxx-devel \
        llvm3.9 \
        llvm3.9-static \
        llvm3.9-devel \
        postgresql

    # Manually download some packages to guarantee
    # version compatibility
    for pkg_path in ${PKGS[@]}; do
        install_package $pkg_path
    done

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
