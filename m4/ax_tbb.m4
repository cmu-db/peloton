AC_DEFUN([AX_TBB], [
    found=false
    AC_ARG_WITH([tbb],
        [AS_HELP_STRING([--with-tbb=DIR],
            [root of the TBB directory])],
        [
            case $withval in
            "" | y | ye | yes | n | no)
                AC_MSG_ERROR([Invalid --with-tbb value])
                ;;
            *)
                tbbdirs="$withval"
            esac
        ],
        [
            # if pkg-config is installed and tbb has installed a .pc file,
            # then use that information and don't search tbbdirs
            AC_PATH_PROG([PKG_CONFIG], [pkg-config])
            if test x"$PKG_CONFIG" != x""; then
                TBB_LDFLAGS=`$PKG_CONFIG tbb --libs-only-L 2>/dev/null`
                if test $? = 0; then
                    TBB_LIBS=`$PKG_CONFIG tbb --libs-only-l 2>/dev/null`
                    TBB_CPPFLAGS=`$PKG_CONFIG tbb --cflags-only-I 2>/dev/null`
                    found=true
                fi
            fi
            # no such luck; use some default tbbdirs
            if ! $found; then
                tbbdirs="/usr /usr/local /opt /opt/local"
            fi
        ]
    )

    # note that we #include <tbb/foo.h>, so the TBB headers have to be in
    # a 'tbb' subdirectory

    if ! $found; then
        TBB_CPPFLAGS=
        for tbbdir in $tbbdirs; do
            AC_MSG_CHECKING([for tbb/tbb.h in $tbbdir])
            if test -f "$tbbdir/include/tbb/tbb.h"; then
                TBB_CPPFLAGS="-I$tbbdir/include"
                TBB_LDFLAGS="-L$tbbdir/lib"
                TBB_LIBS="-ltbb"
                found=true
                AC_MSG_RESULT([yes])
                break
            else
                AC_MSG_RESULT([no])
            fi
        done

        # if the file wasn't found, well, go ahead and try the link anyway -- maybe
        # it will just work!
    fi

    # try the preprocessor and linker with our new flags,
    # being careful not to pollute the global LIBS, LDFLAGS, and CPPFLAGS

    AC_MSG_CHECKING([whether compiling and linking against TBB works])
    echo "Trying link with TBB_LDFLAGS=$TBB_LDFLAGS;" \
        "TBB_LIBS=$TBB_LIBS; TBB_CPPFLAGS=$TBB_CPPFLAGS" >&AS_MESSAGE_LOG_FD

    save_LIBS="$LIBS"
    save_LDFLAGS="$LDFLAGS"
    save_CPPFLAGS="$CPPFLAGS"
    LDFLAGS="$LDFLAGS $TBB_LDFLAGS"
    LIBS="$TBB_LIBS $LIBS"
    CPPFLAGS="$TBB_CPPFLAGS $CPPFLAGS"
	AC_LANG_PUSH([C++])

    AC_LINK_IFELSE(
        [AC_LANG_PROGRAM([#include <tbb/tbb.h>],
			[tbb::concurrent_hash_map<int,int>()])],
        [
            AC_MSG_RESULT([yes])
            $1
        ], [
            AC_MSG_RESULT([no])
            $2
        ])
    CPPFLAGS="$save_CPPFLAGS"
    LDFLAGS="$save_LDFLAGS"
    LIBS="$save_LIBS"
	AC_LANG_POP([C++])

    AC_SUBST([TBB_CPPFLAGS])
    AC_SUBST([TBB_LIBS])
    AC_SUBST([TBB_LDFLAGS])

])
