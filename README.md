# N-Store 

DBMS designed for next-generation storage technologies, like non-volatile memory (NVM), and hybrid transaction/analytical processing (HTAP) workloads.

## Dependencies

> - **g++ 4.7+** 
> - **autotools** 
> - **autoconf** 
> - **tbb** [Thread Building Blocks parallelism library]
> - **json-spirit** [C++ JSON parser/generator]
> - **libreadline-dev** [ Readline library ]
> - **zlib1g-dev** [ Zlib compression library ]
> - **flex** [ Lexical analyzer generator ]
> - **bison** [ Parser generator ]

## Installation 
 
###	Ubuntu Quick Setup

    sudo apt-get install g++ autotools-dev autoconf libtool 
    libtbb-dev libjson-spirit-dev libreadline-dev zlib1g-dev
    flex bison

 
### Get the repository

    git clone https://github.com/cmu-db/n-store.git
 
    cd n-store
 
### First, setup postgres build dir and build

    cd postgres
    mkdir build

    cd build
    ../configure
    make -j4
    sudo make -j4 install
 
    cd ../..

### Now, build N-Store in the repo's root directory

    ./bootstrap

    mkdir build
    cd build
    ../configure CXXFLAGS="-O0 -g" 
    make -j4

### Setup links to nstore library in postgres build and install dirs

These commands depend on the location of the n-store repo dir.
Assuming that the repo's dir is `~/git/n-store/` :

    ln -s  ~/git/n-store/build/src/.libs/libnstore.so ../postgres/build/contrib/nstore/libnstore.so 

    sudo ln -s ~/git/n-store/build/src/.libs/libnstore.so /usr/local/pgsql/lib/libnstore.so 

### Build hooks

    cd ../postgres/contrib/nstore

    make 
    sudo make install

    cd ..

### Create a data dir within postgres build dir

    initdb ./data

    cp ../postgresql.conf ./data   

    pg_ctl -D ./data start

### Try out postgres terminal

    createuser -s -r postgres
    psql postgres 

    help
    
   
### Now, try running this query 

    SELECT * FROM pg_tables
    \q

    pg_ctl -D ./data stop
    
    vi data/pg_log/postgresql.log 

If everything worked fine, you should see the query plan in the log. 

### (Optional) Tweak postgres config file and restart

    vi ./data/postgresql.conf    
    
    pg_ctl -D ./data restart
 
## Development        

###  Environment Setup 

We use `Eclipse` for our development. The following instructions assume that you have installed `EGit` plug-in to enable Eclipse to work with Git repositories and have correctly configured your SSH keys in Github to enable passwordless access.

> 1.    Create a new workspace for Eclipse (if necessary)
> 2.    Select File -> Import -> Git -> Projects from Git.
> 3.    When the next panel comes up, click the `Clone` button. In the next window, put in the path to the Github repository into the URI
> field under Location:    `git@github.com:cmu-db/n-store.git`. Click
> Next.
> 4.    In the next panel you can select which branches you wish to clone from the remote repository. You most likely only need to clone
> `master`. Click Next.
> 5.    Select the location on your local machine that you wish to store your cloned repository. You can leave the other defaults. Click
> Finish.
> 6.   It will now begin to pull down the repository. Once it’s finish, you can then select `n-store` and click Next.
> 7.    On the next panel, select the Import Existing Projects option at the top and click Next.
> 8.    Make sure the `n-store` checkbox is checked on the next page and click Finish.

### Source Code Formatting

> 1. The following steps will configure Eclipse to use the default formatting specifications for `n-store`.
> 2. Open the `n-store` project in Eclipse and navigate to Project ->  Properties ->  C/C++ General.
> 3. Click the Formatter option in the left-hand menu, and then select the Enable project specific settings checkbox at the top of the panel.
> Click the Import button and then select the `eclipse-cpp-google-style.xml` file in `third_party/eclipse` directory.
 
 
### Default workflow (after making changes in N-Store)

    cd build

    make -j4

    cd ../postgres/build

We need to restart the PostgreSQL server to load the updated n-store library.

    pg_ctl -D ./data restart
 
    psql postgres 

## Testing

    cd build
    make -j4 check

    cd tests
    make check-valgrind          
    
### Code Coverage ::

    cd build
    ../configure --enable-code-coverage
    cd test
    make check-code-coverage
 
