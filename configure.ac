# configure.in -- Process this file with autoconf to produce configure
AC_PREREQ([2.64])

AC_INIT([Peloton], [0.0.1], [peloton-dev@cs.cmu.edu])
AC_CONFIG_HEADERS([config.h:config-h.in])
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE([1.0 foreign subdir-objects])

# Silent make by default
AM_SILENT_RULES([yes])

# Enable static library
#AC_PROG_LIBTOOL
# Disable static library
LT_INIT([disable-static])

# Check for pthreads 
ACX_PTHREAD([], [AC_MSG_ERROR([Please install pthread library : libpthread-stubs0-dev])])

# Do not allow AC_PROG_CXX set a default CXXFLAGS
: ${CXXFLAGS=""}

AC_LANG([C++])
AC_PROG_CXX([g++])

AC_PROG_YACC
AC_PROG_LEX
AM_PATH_PYTHON

######################################################################
# LIBRARIES : MM, DL, RT, and M
######################################################################

AC_CHECK_LIB([m], [cos], ,
             AC_MSG_ERROR([Please install libm library], 1))

AC_CHECK_LIB([dl], [dlsym], ,
             AC_MSG_ERROR([Please install libdl library], 1))

AC_CHECK_LIB([rt], [clock_gettime], ,
             AC_MSG_ERROR([Please install librt library], 1))

AC_CHECK_LIB([dw], [dwarf_filesrc], ,
             AC_MSG_ERROR([Please install libdw library : libdw-dev], 1))

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
DX_INIT_DOXYGEN([$PACKAGE_NAME],[doc/doxygen.cfg],[doc])

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

# Check for readline support
AX_LIB_READLINE

# Check for zlib
AX_CHECK_ZLIB([], AC_MSG_ERROR([Please install zlib library : zlib1g-dev]))

# Check for OpenSSL
AX_CHECK_SSL([], AC_MSG_ERROR([Please install ssl library : libssl-dev]))

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

######################################################################
# Debug Compliation Support
######################################################################
AC_MSG_CHECKING([whether to build with debug information])
AC_ARG_ENABLE([debug],
              [AS_HELP_STRING([--enable-debug],
                              [enable debug data generation (def=no)])],
                              [debugit="$enableval"],
                              [debugit=no])
AC_MSG_RESULT([$debugit])

if test x"$debugit" = x"yes"; then
    AC_DEFINE([DEBUG],[],[Debug Mode])
    DEBUG_CXXFLAGS="-g -O0 -Wall -Wextra -Werror"
    DEBUG_CPPFLAGS=""
else
    AC_DEFINE([NDEBUG],[],[No-debug Mode])
    DEBUG_CXXFLAGS="-O2 "
    DEBUG_CPPFLAGS="-DNDEBUG"
fi

AC_SUBST([DEBUG_CXXFLAGS])
AC_SUBST([DEBUG_CPPFLAGS])



#####################################################################
# Workaround for Code Coverage
#####################################################################
AC_CONFIG_LINKS([src/scan.cpp:src/postgres/backend/parser/scan.cpp])
AC_CONFIG_LINKS([src/postgres/backend/parser/gram.cpp:src/postgres/backend/parser/gram.cpp])
AC_CONFIG_LINKS([src/guc-file.cpp:src/postgres/backend/utils/misc/guc-file.cpp])
AC_CONFIG_LINKS([src/postgres/backend/bootstrap/bootparse.cpp:src/postgres/backend/bootstrap/bootparse.cpp])
AC_CONFIG_LINKS([src/postgres/backend/bootstrap/bootparse.y:src/postgres/backend/bootstrap/bootparse.y])
AC_CONFIG_LINKS([src/postgres/backend/bootstrap/bootscanner.cpp:src/postgres/backend/bootstrap/bootscanner.cpp])
AC_CONFIG_LINKS([src/postgres/backend/bootstrap/bootscanner.l:src/postgres/backend/bootstrap/bootscanner.l])
AC_CONFIG_LINKS([src/postgres/backend/replication/repl_gram.cpp:src/postgres/backend/replication/repl_gram.cpp])
AC_CONFIG_LINKS([src/postgres/backend/replication/repl_gram.y:src/postgres/backend/replication/repl_gram.y])
AC_CONFIG_LINKS([src/postgres/backend/replication/repl_scanner.cpp:src/postgres/backend/replication/repl_scanner.cpp])
AC_CONFIG_LINKS([src/postgres/backend/replication/repl_scanner.l:src/postgres/backend/replication/repl_scanner.l])
AC_CONFIG_LINKS([src/postgres/backend/replication/fmgrtab.cpp:src/postgres/backend/utils/fmgrtab.cpp])
AC_CONFIG_LINKS([src/postgres/backend/utils/sort/qsort_tuple.cpp:src/postgres/backend/utils/sort/qsort_tuple.cpp])
AC_CONFIG_LINKS([tools/pg_psql/sql_help.cpp:tools/pg_psql/sql_help.cpp])
AC_CONFIG_LINKS([tools/pg_psql/psqlscan.cpp:tools/pg_psql/psqlscan.cpp])

AC_OUTPUT

echo \
    "---------------------------------------------------------------------

${PACKAGE_NAME}

Version        : ${PACKAGE_VERSION}
OS             : ${build_os}
Prefix         : ${prefix}
Compiler       : ${CC}
CPP Flags      : ${CPPFLAGS} ${DEBUG_CPPFLAGS}
C++ Flags      : ${CXXFLAGS} ${DEBUG_CXXFLAGS}
Make Flags     : ${MAKEFLAGS}
Libraries      : ${LIBS}

---------------------------------------------------------------------"
