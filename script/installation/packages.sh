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
##  * Ubuntu (14.04, 16.04, 18.04)
##  * macOS
##
## Update (2018-06-08):
## We are no longer able to support RedHat/Fedora because those
## environments are not supported by TensorFlow.
## =================================================================

set -o errexit

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
TF_TYPE="cpu"


function install_protobuf3.4.0() {
    # Install Relevant tooling
    # Remove any old versions of protobuf
    # Note: Protobuf 3.5+ PPA available Ubuntu Bionic(18.04) onwards - Should be used
    # when we retire 16.04 too: https://launchpad.net/~maarten-fonville/+archive/ubuntu/protobuf
    # This PPA unfortunately doesnt have Protobuf 3.5 for 16.04, but does for 14.04/18.04+
    DISTRIB=$1 # ubuntu/fedora
    if [ "$DISTRIB" == "ubuntu" ]; then
        sudo apt-get --yes --force-yes remove --purge libprotobuf-dev protobuf-compiler
    elif [ "$DISTRIB" == "fedora" ]; then
        sudo dnf -q remove -y protobuf protobuf-devel protobuf-compiler
    else
        echo "Only Ubuntu and Fedora is supported currently!"
        return 0
    fi
    if /usr/local/bin/protoc --version == "libprotoc 3.4.0"; then
        echo "protobuf-3.4.0 already installed"
        return
    fi
    wget -O protobuf-cpp-3.4.0.tar.gz https://github.com/google/protobuf/releases/download/v3.4.0/protobuf-cpp-3.4.0.tar.gz
    tar -xzf protobuf-cpp-3.4.0.tar.gz
    cd protobuf-3.4.0
    ./autogen.sh && ./configure && make -j4 && sudo make install && sudo ldconfig || exit 1
    cd ..
    # Cleanup
    rm -rf protobuf-3.4.0 protobuf-cpp-3.4.0.tar.gz
}

# Utility function for installing tensorflow components of python/C++
function install_tf() {
    if pip show -q tensorflow && [ -d /usr/local/include/tensorflow/c ]; then
        echo "tensorflow already installed"
        return
    fi
    TFCApiFile=$1
    TF_VERSION=$2
    LinkerConfigCmd=$3
    TARGET_DIRECTORY="/usr/local"
    # Install Tensorflow Python Binary
    sudo -E pip3 install --upgrade pip
    # Related issue: https://github.com/pypa/pip/issues/3165
    sudo -E pip3 install tensorflow==${TF_VERSION} --upgrade --ignore-installed six

    # Install C-API
    TFCApiURL="https://storage.googleapis.com/tensorflow/libtensorflow/${TFCApiFile}"
    wget -O $TFCApiFile $TFCApiURL
    sudo tar -C $TARGET_DIRECTORY -xzf $TFCApiFile || true
    # Configure the Linker
    eval $LinkerConfigCmd
    # Cleanup
    rm -rf ${TFCApiFile}
}

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
        if [ "$MAJOR_VER" == "18" ]; then
            LLVM_PKG_URL="http://apt.llvm.org/bionic/"
            LLVM_PKG_TARGET="llvm-toolchain-bionic main"
        fi

        if ! grep -q "deb $LLVM_PKG_URL $LLVM_PKG_TARGET" /etc/apt/sources.list; then
            echo -e "\n# Added by Peloton 'packages.sh' script on $(date)\ndeb $LLVM_PKG_URL $LLVM_PKG_TARGET" | sudo tee -a /etc/apt/sources.list > /dev/null
        fi
        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 15CF4D18AF4F7421
        CMAKE_NAME="cmake3"
    else
        CMAKE_NAME="cmake"
    fi

    sudo apt-get update
    FORCE_Y=""
    PKG_CMAKE="cmake"
    PKG_LLVM="llvm-3.7"
    PKG_CLANG="clang-3.7"
    TF_VERSION="1.4.0"

    # Fix for cmake name change on Ubuntu 14.x and 16.x plus --force-yes deprecation
    if [ "$MAJOR_VER" == "14" ]; then
        PKG_CMAKE="cmake3"
        FORCE_Y="--force-yes"
        TF_VERSION="1.4.0"
    fi
    if [ "$MAJOR_VER" == "16" ]; then
        TF_VERSION="1.5.0"
    fi
    # Fix for llvm on Ubuntu 17.x
    if [ "$MAJOR_VER" == "17" ]; then
        PKG_LLVM="llvm-3.9"
        PKG_CLANG="clang-3.8"
        TF_VERSION="1.5.0"
    fi
    # Fix for llvm on Ubuntu 18.x
    if [ "$MAJOR_VER" == "18" ]; then
        PKG_LLVM="llvm-3.9"
        PKG_CLANG="clang-3.9"
        TF_VERSION="1.5.0"
    fi
    TFCApiFile="libtensorflow-${TF_TYPE}-linux-x86_64-${TF_VERSION}.tar.gz"
    LinkerConfigCmd="sudo ldconfig"
    sudo apt-get -q $FORCE_Y --ignore-missing -y install \
        $PKG_CMAKE \
        $PKG_LLVM \
        $PKG_CLANG \
        git \
        g++ \
        bison \
        flex \
        valgrind \
        lcov \
        libgflags-dev \
        libevent-dev \
        libboost-dev \
        libboost-thread-dev \
        libboost-filesystem-dev \
        libjemalloc-dev \
        libpqxx-dev \
        libedit-dev \
        libssl-dev \
        postgresql-client \
        libffi6 \
        libffi-dev \
        libtbb-dev \
        python3-pip \
        curl \
        autoconf \
        automake \
        libtool \
        make \
        g++ \
        libeigen3-dev \
    	ant \
        unzip \
        zlib1g-dev
    # Install version of protobuf needed by C-API
    install_protobuf3.4.0 "ubuntu"
    # Install tensorflow
    install_tf "$TFCApiFile" "$TF_VERSION" "$LinkerConfigCmd"

## ------------------------------------------------
## DARWIN (macOS)
## ------------------------------------------------
elif [ "$DISTRO" = "DARWIN" ]; then
    set +o errexit
    if test ! $(which brew); then
      echo "Installing homebrew..."
      ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    fi
    TF_VERSION="1.4.0"
    TFCApiFile="libtensorflow-${TF_TYPE}-darwin-x86_64-${TF_VERSION}.tar.gz"
    LinkerConfigCmd="sudo update_dyld_shared_cache"
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
    brew install libffi
    brew install tbb
    brew install curl
    brew install wget
    brew install python
    brew upgrade python
    brew install eigen
    brew install ant
    # Brew installs correct version of Protobuf(3.5.1 >= 3.4.0)
    # So we can directly install tensorflow
    install_tf "$TFCApiFile" "$TF_VERSION" "$LinkerConfigCmd"

## ------------------------------------------------
## UNKNOWN
## ------------------------------------------------
else
    echo "Unsupported distribution '$DISTRO'"
    echo "Please contact our support team for additional help." \
         "Be sure to include the contents of this message"
    echo "Platform: $(uname -a)"
    echo
    echo "https://github.com/cmu-db/peloton/issues"
    echo
    exit 1
fi
