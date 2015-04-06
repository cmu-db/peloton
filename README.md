# N-Store 
DBMS designed for next-generation *storage technologies*, like non-volatile memory (NVM), and hybrid transaction/analytical processing (HTAP) *workloads* that enable real-time analytics.

## Dependencies

> - **g++ 4.7+** 
> - **autotools** 
> - **autoconf** 
> - **tbb** [Thread Building Blocks - Parallelism library]
> - **json-spirit** [C++ JSON Parser/Generator]
> - **gdamm** [C++ wrappers for GNU Data Access library]

###	Ubuntu Quick Setup
    sudo apt-get install g++ autotools-dev autoconf libtool 
    libtbb-dev libjson-spirit-dev libgdamm5.0-dev

## Deployment

    ./bootstrap
    cd build
    ../configure  
    make -j4
    make check [optional]

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

## Test

### Code Coverage ::

    cd build
    ../configure --enable-code-coverage
    cd test
    make check-code-coverage

### Valgrind ::

    cd test
    make check-valgrind
