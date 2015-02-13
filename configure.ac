# configure.in -- Process this file with autoconf to produce configure
AC_PREREQ([2.64])

AC_INIT([n-store], [0.1], [jarulraj@cs.cmu.edu])
AC_CONFIG_HEADERS([config.h:config-h.in])
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE([1.0 foreign])

AC_PROG_CC

if test -z $CXXFLAGS; then
    CXXFLAGS='-ggdb -O3'
fi

AC_PROG_CXX
AC_PROG_LIBTOOL

# Check for pthreads (uses m4/acx_pthread.m4).
ACX_PTHREAD([], [AC_MSG_ERROR([failed to find pthreads])])

AC_LANG([C++])

# turn on c++11
AX_CXX_COMPILE_STDCXX_11(noext)

#BOOST_REQUIRE([1.45])
#BOOST_SYSTEM
#BOOST_ASIO
#BOOST_THREADS
#BOOST_SERIALIZATION

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 test/Makefile])                  
                
#[test -z "$CONFIG_HEADERS" || echo timestamp > stamp-h.in])
AC_OUTPUT
# @end 2
