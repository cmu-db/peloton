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
# OS X portability 
######################################################################
AM_CONDITIONAL(BUILD_OS_IS_DARWIN, [test x"$build_os" = darwin])

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

PKG_CHECK_MODULES([TBB], [tbb >= 1.0.0])

AC_SUBST([TBB_CFLAGS])
AC_SUBST([TBB_LIBS])

#[test -z "$CONFIG_HEADERS" || echo timestamp > stamp-h.in])

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 tests/Makefile
                 ])

AC_CONFIG_LINKS([tests/valgrind.supp:third_party/valgrind/valgrind.supp])

AC_OUTPUT
# @end 2
