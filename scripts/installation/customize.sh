#!/bin/sh

# Install prerequisites
sudo apt-get -y install eclipse eclipse-cdt tmux zsh gdb valgrind linux-tools-generic vim emacs24-nox python-virtualenv
sudo apt-get -y --no-install-recommends install doxygen graphviz

# Clone Oh My Zsh from the git repo
git clone git://github.com/robbyrussell/oh-my-zsh.git ~/.oh-my-zsh

# Copy in the default .zshrc config file
cp ~/.oh-my-zsh/templates/zshrc.zsh-template ~/.zshrc

# Change the vagrant user's shell to use zsh
sudo chsh -s /bin/zsh vagrant

# Add peloton install dir to the path
export PATH=$PATH:/usr/local/peloton/bin
