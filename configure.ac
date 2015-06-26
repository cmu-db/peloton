# configure.in -- Process this file with autoconf to produce configure
AC_PREREQ([2.64])

AC_INIT([Peloton], [0.0.1], [peloton-dev@cs.cmu.edu])
AC_CONFIG_HEADERS([config.h:config-h.in])
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE([1.0 foreign subdir-objects])

# Silent make by default
AM_SILENT_RULES([yes])

AC_PROG_CC([gcc])
# Enable static library
#AC_PROG_LIBTOOL
# Disable static library
LT_INIT([disable-static])

# Check for pthreads 
ACX_PTHREAD([], [AC_MSG_ERROR([Please install pthread library : libpthread-stubs0-dev])])

AC_LANG([C++])
AC_PROG_CXX([g++])
AC_SUBST([AM_CXXFLAGS], '-std=c++11 -stdlib=libc++')

######################################################################
# OS X Support
######################################################################

AM_CONDITIONAL(BUILD_OS_IS_DARWIN, [[[[ "$build_os" == darwin* ]]]])

######################################################################
# DOXYGEN SUPPORT
######################################################################

DX_HTML_FEATURE(ON)
DX_DOT_FEATURE(OFF)
DX_PDF_FEATURE(OFF)
DX_PS_FEATURE(OFF)
DX_INIT_DOXYGEN([$PACKAGE_NAME],[doxygen.cfg],[doc])

######################################################################
# GCC COLOR
######################################################################

AX_CHECK_COMPILE_FLAG([-fdiagnostics-color=auto], [CXXFLAGS="$CXXFLAGS -fdiagnostics-color=auto"])

######################################################################
# VALGRIND + GCOV
######################################################################

AX_VALGRIND_CHECK
AX_CODE_COVERAGE

######################################################################
# PARALLEL MAKE 
######################################################################

NPROCS=1
if [[ "$build_os" == "linux-gnu" ]]
then
    NPROCS=$(grep -c ^processor /proc/cpuinfo)
elif [[ "$build_os" == "darwin" ]]
then
    NPROCS=$(sysctl -n hw.physicalcpu)
fi

#NPROCS=`expr $NPROCS / 2`
MAKEFLAGS="-j$NPROCS"

######################################################################
# PACKAGES
######################################################################

# Check for flex and bison
AX_PROG_FLEX([], AC_MSG_ERROR([Please install flex : flex]))
AX_PROG_BISON([], AC_MSG_ERROR([Please install bison : bison]))

# Check for readline support
AX_LIB_READLINE

# Check for zlib
AX_CHECK_ZLIB([], AC_MSG_ERROR([Please install zlib library : libzlib1g-dev]))

# Check for OpenSSL
AC_CHECK_SSL

# Check for Intel TBB library
AX_TBB([], AC_MSG_ERROR([Please install the Intel TBB library : libtbb-dev]))

# Check for Boost libraries
#AX_BOOST_BASE([1.42])
#AX_BOOST_ASIO
#AX_BOOST_FILESYSTEM
#AX_BOOST_SERIALIZATION
#AX_BOOST_SYSTEM
#AX_BOOST_THREAD

#[test -z "$CONFIG_HEADERS" || echo timestamp > stamp-h.in])

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 tests/Makefile
                 tools/Makefile
                 ])

AC_CONFIG_LINKS([tests/valgrind.supp:third_party/valgrind/valgrind.supp])

AC_OUTPUT

echo \
"---------------------------------------------------------------------

${PACKAGE_NAME} 

Version        : ${PACKAGE_VERSION}
OS             : ${build_os}
Prefix         : ${prefix}
Compiler       : ${CC}
C Flags        : ${CFLAGS}
CPP Flags      : ${CPPFLAGS}
C++ Flags      : ${CXXFLAGS}
Make Flags     : ${MAKEFLAGS}
Libraries      : ${LIBS} $TBB_LIBS

---------------------------------------------------------------------"
