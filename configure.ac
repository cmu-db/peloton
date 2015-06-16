# configure.in -- Process this file with autoconf to produce configure
AC_PREREQ([2.64])

AC_INIT([peloton], [0.0.1], [jarulraj@cs.cmu.edu])
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

# Check for pthreads (uses m4/acx_pthread.m4).
ACX_PTHREAD([], [AC_MSG_ERROR([Could not find pthreads in your system.])])

AC_LANG([C++])
AC_PROG_CXX([g++])
AC_SUBST([AM_CXXFLAGS], '-std=c++11 -stdlib=libc++')


######################################################################
# OS X portability support
######################################################################
AM_CONDITIONAL(BUILD_OS_IS_DARWIN, [[[[ "$build_os" == darwin* ]]]])

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
# GCC COLOR
######################################################################

AX_CHECK_COMPILE_FLAG([-fdiagnostics-color=auto], [CXXFLAGS="$CXXFLAGS -fdiagnostics-color=auto"])

######################################################################
# VALGRIND + GCOV
######################################################################

AX_VALGRIND_CHECK
AX_CODE_COVERAGE

######################################################################
# PARSER
######################################################################

AC_PROG_LEX
AC_PROG_YACC

######################################################################
# PACKAGES
######################################################################

# Check for OpenSSL
AC_CHECK_SSL

# Check for Intel TBB library
AX_TBB([], AC_MSG_ERROR([cannot find the Intel TBB library]))


# Checks for Boost libraries
AX_BOOST_BASE([1.42])

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
OS             : '${build_os}'
Prefix         : '${prefix}'.
Compiler       : '${CC} ${CFLAGS} ${CPPFLAGS}'
Libraries      : '${LIBS}' '$TBB_LIBS'
TBB_CPPFLAGS   : '${TBB_CPPFLAGS}'
TBB_LDFLAGS    : '${TBB_LDFLAGS}'
                    


---------------------------------------------------------------------"

# @end 2
