# Peloton

DBMS designed for next-generation storage technologies, like non-volatile memory (NVM), and hybrid transaction/analytical processing (HTAP) workloads.

## Dependencies

> - **g++ 4.7+** [ Compiler, need support for C++11 ] 
> - **libtool** 
> - **pkg-config** 
> - **tbb** [Thread Building Blocks parallelism library]
> - **json-spirit** [C++ JSON parser/generator]
> - **readline** [Commandline editing library]
> - **valgrind** [ Dynamic analysis framework ]
> - **mm** [ Shared memory library ]

## Installation 
 
###	Ubuntu Quick Setup

    sudo apt-get install g++ pkg-config libtool libtbb-dev libjson-spirit-dev libreadline-dev libmm 

### OS X Setup

    brew install automake tbb json_spirit 
 
### Build Peloton

    git clone https://github.com/cmu-db/peloton.git

    ./bootstrap
    
    cd build
    ../configure 
    make -j4
    sudo make -j4 install

### Update paths 

    export PATH=$PATH:/usr/local/peloton/bin
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

### Test terminal

    initdb ./data
    cp ../postgresql.conf ./data   
    
    pg_ctl -D ./data start
    createuser -s -r postgres
    
    psql postgres 
    help  
    
    pg_ctl -D ./data stop

### Testing

    make check -j4        // Build and execute tests
    cd tests; make check-build -j4  // Only build the tests.

## Development        

###  Environment Setup 

We use `Eclipse` for our development. The following instructions assume that you have installed `EGit` plug-in to enable Eclipse to work with Git repositories and have correctly configured your SSH keys in Github to enable passwordless access.

> 1.    Create a new workspace for Eclipse (if necessary)
> 2.    Select File -> Import -> Git -> Projects from Git.
> 3.    When the next panel comes up, click the `Clone` button. In the next window, put in the path to the Github repository into the URI
> field under Location:    `git@github.com:cmu-db/peloton.git`. Click
> Next.
> 4.    In the next panel you can select which branches you wish to clone from the remote repository. You most likely only need to clone
> `master`. Click Next.
> 5.    Select the location on your local machine that you wish to store your cloned repository. You can leave the other defaults. Click
> Finish.
> 6.   It will now begin to pull down the repository. Once it’s finish, you can then select `peloton` and click Next.
> 7.    On the next panel, select the Import Existing Projects option at the top and click Next.
> 8.    Make sure the `peloton` checkbox is checked on the next page and click Finish.

### Source Code Formatting

> 1. The following steps will configure Eclipse to use the default formatting specifications for `peloton`.
> 2. Open the `peloton` project in Eclipse and navigate to Project ->  Properties ->  C/C++ General.
> 3. Click the Formatter option in the left-hand menu, and then select the Enable project specific settings checkbox at the top of the panel.
> Click the Import button and then select the `eclipse-cpp-google-style.xml` file in `third_party/eclipse` directory.
