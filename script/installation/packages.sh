#!/bin/bash -x

# Determine OS platform
UNAME=$(uname | tr "[:upper:]" "[:lower:]")
# If Linux, try to determine specific distribution
if [ "$UNAME" == "linux" ]; then
    # If available, use LSB to identify distribution
    if [ -f /etc/lsb-release -o -d /etc/lsb-release.d ]; then
        export DISTRO=$(lsb_release -i | cut -d: -f2 | sed s/'^\t'//)
        DISTRO_VER=$(lsb_release -r | cut -d: -f2 | sed s/'^\t'//)
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

function install_repo_package() {
    if [ "$#" -ne 1 ]; then
        echo "The download path is required."
        exit 1 
    else
        dpath=$(basename "$1")
    fi
    pushd $TMPDIR
    wget -nc --no-check-certificate "$1"
    sudo yum install -y "$dpath"
    popd
    return 0
}

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


## ------------------------------------------------
## UBUNTU
## ------------------------------------------------
if [ "$DISTRO" = "UBUNTU" ]; then
    # Fix for LLVM-3.7 on Ubuntu 14.04
    if [ "$DISTRO_VER" == "14.04" ]; then
        sudo add-apt-repository 'deb http://llvm.org/apt/trusty/ llvm-toolchain-trusty-3.7 main'
        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 15CF4D18AF4F7421
        sudo apt-get update
    fi

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
        llvm-3.7 \
        libedit-dev \
        postgresql-client

## ------------------------------------------------
## FEDORA
## ------------------------------------------------
elif [[ "$DISTRO" == *"FEDORA"* ]]; then
    sudo dnf install -y git \
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
        libpqxx-devel \
        libpqxx \
        llvm3.7 \
        libedit-devel \
        postgresql

## ------------------------------------------------
## REDHAT
## ------------------------------------------------
elif [[ "$DISTRO" == *"REDHAT"* ]] && [[ "${DISTRO_VER%.*}" == "7" ]]; then
    # Package download paths
    PKG_REPOS=(
        "https://download.postgresql.org/pub/repos/yum/9.3/redhat/rhel-7-x86_64/pgdg-redhat93-9.3-3.noarch.rpm"
    )
    PKGS=(
        "https://github.com/downloads/libevent/libevent/libevent-2.0.21-stable.tar.gz"
        "http://ftp.gnu.org/gnu/bison/bison-3.0.tar.gz"
        "https://github.com/schuhschuh/gflags/archive/v2.0.tar.gz"
        "https://sourceforge.net/projects/boost/files/boost/1.54.0/boost_1_54_0.tar.gz"
    )

    # Add required repos
    for repo_path in ${PKG_REPOS[@]}; do
        install_repo_package $repo_path
    done

    # Simple installations via yum
    sudo yum install -y \
        git \
        gcc-c++ \
        cmake \
        flex \
        protobuf-devel \
        jemalloc-devel \
        valgrind \
        lcov \
        m4 \
        doxygen \
        graphviz \
        libpqxx-devel \
        llvm3.7 \
        libedit-devel \
        postgresql93

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
