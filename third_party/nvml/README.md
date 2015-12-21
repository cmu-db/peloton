nvml: Linux NVM Library
=======================

[![Build Status](https://travis-ci.org/pmem/nvml.svg)](https://travis-ci.org/pmem/nvml)

This is the top-level README.md the Linux NVM Library.
For more information, see http://pmem.io.

|**NOTE**|
|:------:|
|**These libraries are not yet considered production quality, but they are getting close!  They are currently validated to "alpha" quality, meaning all features pass their tests with no known critical issues.  You are encouraged to try them out and give us feedback via our [Google group](http://groups.google.com/group/pmem).**|

### The Libraries ###

Please see the file [LICENSE](https://github.com/pmem/nvml/blob/master/LICENSE) for information on how this library is licensed.

This tree contains a collection of libraries for using Non-Volatile Memory
(NVM).  There are currently six libraries:

* **libpmem** -- basic pmem operations like flushing
* **libpmemblk**, **libpmemlog**, **libpmemobj** -- pmem transactions
* **libvmem**, **libvmmalloc** -- volatile use of pmem

These libraries are described in more detail on the
[pmem web site](http://pmem.io).  There you'll find man pages, examples,
and tutorials.

**Currently, these libraries only work on 64-bit Linux.**

### Pre-Built Packages ###

If you want to install these libraries to try them out of your system, you can
either install pre-built packages, which we build for every stable release, or
clone the tree and build it yourself.

Builds are tagged something like `0.2+b1`, which means
*Build 1 on top of version 0.2* and `0.2-rc3`, which means
*Release Candidate 3 for version 0.2*.  **Stable** releases
are the simpler *major.minor* tags like `0.2`.  To find
pre-build packages, check the Downloads associated with
the stable releases on the
[github release page](https://github.com/pmem/nvml/releases).

### Building The Source ###

The source tree is organized as follows:

* **doc** -- man pages describing each library contained here
* **src** -- the source for the libraries
* **src/benchmarks** -- benchmarks used by development team
* **src/examples** -- brief example programs using these libraries
* **src/test** -- unit tests used by development team
* **src/tools** -- various tools developed for NVML
* **utils** -- utilities used during build & test
* **CONTRIBUTING.md** -- instructions for people wishing to contribute

To build this library, you may need to install some required packages on
the build system.  See the **before_install:** rules in the
[.travis.yml](https://github.com/pmem/nvml/blob/master/.travis.yml)
file at the top level of the repository to get an idea what packages
were required to build on the _travis-ci_ (Ubuntu-based) systems.

To build the latest development version, just clone this tree and build the master branch:
```
	$ git clone https://github.com/pmem/nvml
	$ cd nvml
```

Once the build system is setup, the NVM Library is built using
this command at the top level:
```
	$ make
```

Once the make completes (*), all the libraries are built and the examples
under `src/examples` are built as well.  You can play with the library
within the build tree, or install it locally on your machine.  Installing
the library is more convenient since it installs man pages and libraries
in the standard system locations:
```
	(as root...)
	# make install
```

To install this library into other locations, you can use the
prefix variable, e.g.:
```
	$ make install prefix=/usr/local
```
This will install files to /usr/local/lib, /usr/local/include /usr/local/share/man.

To prepare this library for packaging, you can use the
DESTDIR variable, e.g.:
```
	$ make install DESTDIR=/tmp
```
This will install files to /tmp/usr/lib, /tmp/usr/include /tmp/usr/share/man.

To install a complete copy of the source tree to $(DESTDIR)/nvml:
```
	$ make source DESTDIR=some_path
```

To build rpm packages on rpm-based distributions:
```
	$ make rpm
```
**Prerequisites:** rpmbuild

To build dpkg packages on Debian-based distributions:
```
	$ make dpkg
```
**Prerequisites:** devscripts

(*) By default all code is built with -Werror flag which fails the whole build
when compiler emits any warning. It's very useful during development, but can be
annoying in deployment. If you want to disable -Werror, you can use EXTRA_CFLAGS
variable:
```
	$ make EXTRA_CFLAGS="-Wno-error"
```
or
```
	$ make EXTRA_CFLAGS="-Wno-error=$(type-of-warning)"
```

### Testing the Libraries ###

To build and run the unit tests:
```
	$ make check
```

To run a specific subset of tests, run for example:
```
	$ make check TEST_TYPE=short TEST_BUILD=debug TEST_FS=pmem
```

To modify the timeout which is available for **check** type tests, run:
```
	$ make check TEST_TIME=1m
```
This will set the timeout to 1 minute.

Please refer to the **src/test/README** for more details on how to
run different types of tests.

To compile this library with enabled support for the PM-aware version
of [Valgrind](https://github.com/pmem/valgrind), supply the compiler
with the **USE_VG_PMEMCHECK** flag, for example:
```
	$ make EXTRA_CFLAGS=-DUSE_VG_PMEMCHECK
```
For Valgrind memcheck support, supply **USE_VG_MEMCHECK** flag.
**USE_VALGRIND** flag enables both.

To test the libraries with AddressSanitizer and UndefinedBehaviorSanitizer, run:
```
	$ make EXTRA_CFLAGS="-fsanitize=address,undefined" EXTRA_LDFLAGS="-fsanitize=address,undefined" clobber all test check
```

### Contacts ###

For more information on this library,
contact Andy Rudoff (andy.rudoff@intel.com) or post to our
[Google group](http://groups.google.com/group/pmem).
