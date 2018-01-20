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
    MAJOR_VER=$(echo "$DISTRO_VER" | cut -d '.' -f 1)

    # Fix for LLVM-3.7 on Ubuntu 14 + 17
    if [ "$MAJOR_VER" == "14" -o "$MAJOR_VER" == "17" ]; then
        if [ "$MAJOR_VER" == "14" ]; then
            LLVM_PKG_URL="http://llvm.org/apt/trusty/"
            LLVM_PKG_TARGET="llvm-toolchain-trusty-3.7 main"
        fi
        if [ "$MAJOR_VER" == "17" ]; then
            LLVM_PKG_URL="http://apt.llvm.org/artful/"
            LLVM_PKG_TARGET="llvm-toolchain-artful main"
        fi

        if ! grep -q "deb $LLVM_PKG_URL $LLVM_PKG_TARGET" /etc/apt/sources.list; then
            echo -e "\n# Added by Peloton 'packages.sh' script on $(date)\ndeb $LLVM_PKG_URL $LLVM_PKG_TARGET" | sudo tee -a /etc/apt/sources.list > /dev/null
        fi
        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 15CF4D18AF4F7421
        sudo apt-get update -qq
        CMAKE_NAME="cmake3"
        FORCE_Y="--force-yes"
    else
        CMAKE_NAME="cmake"
        FORCE_Y=""
    fi

    FORCE_Y=""
    PKG_CMAKE="cmake"
    PKG_LLVM="llvm-3.7"
    PKG_CLANG="clang-3.7"

    # Fix for cmake name change on Ubuntu 14.x and 16.x plus --force-yes deprecation
    if [ "$MAJOR_VER" == "14" ]; then
        PKG_CMAKE="cmake3"
        FORCE_Y="--force-yes"
    fi
    # Fix for llvm on Ubuntu 17.x
    if [ "$MAJOR_VER" == "17" ]; then
        PKG_LLVM="llvm-3.9"
        PKG_CLANG="clang-3.8"
    fi

    sudo apt-get -qq $FORCE_Y --ignore-missing -y install \
        $PKG_CMAKE \
        $PKG_LLVM \
        $PKG_CLANG \
        git \
        g++ \
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
        libedit-dev \
        postgresql-client

## ------------------------------------------------
## DEBIAN
## ------------------------------------------------
elif [ "$DISTRO" = "DEBIAN OS" ]; then
    sudo apt-get -qq --ignore-missing -y install \
        git \
        g++ \
        clang \
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
        libssl-dev \
        valgrind \
        lcov \
        libpqxx-dev \
        llvm-dev \
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
        cmake3 \
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
