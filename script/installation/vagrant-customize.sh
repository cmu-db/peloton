#!/bin/sh

## =================================================================
## VAGRANT ENVIRONMENT CUSTOMIZATION
##
## This script is invoked automatically by Vagrantfile to
## setup the development environment. It installs the Eclipse IDE.
## Most people probably want clion though.
## The Vagrantfile executes 'packages.sh' before this script.
## =================================================================

# Install prerequisites
sudo apt-get -y install \
    eclipse \
    eclipse-cdt \
    tmux \
    zsh \
    gdb \
    valgrind \
    linux-tools-generic \
    vim \
    emacs24-nox \
    python-virtualenv \
    python-numpy \
    uuid-dev \
    clang-format-3.7 \
    default-jdk \
    default-jre \
    ant \
    kcachegrind \
    scons

sudo apt-get -y --no-install-recommends install \
    doxygen \
    graphviz

# Clone Oh My Zsh from the git repo
git clone git://github.com/robbyrussell/oh-my-zsh.git ~/.oh-my-zsh

# Copy in the default .zshrc config file
cp ~/.oh-my-zsh/templates/zshrc.zsh-template ~/.zshrc

# Change the vagrant user's shell to use zsh
sudo chsh -s /bin/zsh vagrant

# Add peloton install dir to the path
export PATH=$PATH:/usr/local/bin
