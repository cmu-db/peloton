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

# G++ C++11
AX_CXX_COMPILE_STDCXX_11(noext)

#BOOST_REQUIRE([1.45])
#BOOST_SYSTEM
#BOOST_ASIO
#BOOST_THREADS
#BOOST_SERIALIZATION

######################################################################
# DOXYGEN SUPPORT
######################################################################
DX_HTML_FEATURE(ON)
DX_CHM_FEATURE(OFF)
DX_CHI_FEATURE(OFF)
DX_MAN_FEATURE(OFF)
DX_RTF_FEATURE(OFF)
DX_XML_FEATURE(OFF)
DX_PDF_FEATURE(OFF)
DX_PS_FEATURE(OFF)
DX_INIT_DOXYGEN([$PACKAGE_NAME], [doc/doxygen.cfg], [doc])

#[test -z "$CONFIG_HEADERS" || echo timestamp > stamp-h.in])

 AC_CONFIG_FILES([Makefile
                  src/Makefile
                  test/Makefile])            

                  AC_OUTPUT
                  # @end 2
