# configure.in -- Process this file with autoconf to produce configure
AC_PREREQ([2.64])

AC_INIT([n-store], [0.1], [jarulraj@cs.cmu.edu])
AC_CONFIG_HEADERS([config.h:config-h.in])
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE([1.0 foreign subdir-objects])

AC_PROG_CC([gcc])
AC_PROG_LIBTOOL
 
# Check for pthreads (uses m4/acx_pthread.m4).
ACX_PTHREAD([], [AC_MSG_ERROR([Oops, we could not find pthreads in your system.])])

AC_LANG([C++])
AC_PROG_CXX([g++])
AC_SUBST([AM_CXXFLAGS], '-std=c++11 -stdlib=libc++')

######################################################################
# BOOST
######################################################################

BOOST_REQUIRE(1.53)

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

######################################################################
# VALGRIND + GCOV
######################################################################

AX_VALGRIND_CHECK
AX_CODE_COVERAGE

#[test -z "$CONFIG_HEADERS" || echo timestamp > stamp-h.in])

AC_CONFIG_FILES([Makefile
                 third_party/Makefile
                 third_party/gtest/Makefile
                 src/Makefile
                 tests/Makefile
                 ])

AC_OUTPUT
# @end 2
