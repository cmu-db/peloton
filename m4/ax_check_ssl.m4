dnl
dnl Copyright (c) 2008 GeNUA mbH <info@genua.de>
dnl
dnl All rights reserved.
dnl
dnl Redistribution and use in source and binary forms, with or without
dnl modification, are permitted provided that the following conditions
dnl are met:
dnl 1. Redistributions of source code must retain the above copyright
dnl    notice, this list of conditions and the following disclaimer.
dnl 2. Redistributions in binary form must reproduce the above copyright
dnl    notice, this list of conditions and the following disclaimer in the
dnl    documentation and/or other materials provided with the distribution.
dnl
dnl THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
dnl "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
dnl LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
dnl A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
dnl OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
dnl SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
dnl TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
dnl PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
dnl LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
dnl NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
dnl SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
dnl
dnl Synopsis
dnl	AX_CHECK_SSL
dnl
dnl Description
dnl	This macro will check some locations for ssl library.
dnl	The user may provide own suggestions via '--with-ssldir='
dnl
dnl	Within Makefile.am's the following variabls will be substituted:
dnl		SSL_CFLAGS
dnl		SSL_LDFLAGS
dnl		SSL_LIBS
dnl	Example:
dnl		xxx_CFLAGS=@SSL_CFLAGS@ -Iyyy
dnl
dnl	Within source code
dnl		HAVE_SSL
dnl	will be defined, if a library was found.
dnl	Example:
dnl		#ifdef HAVE_SSL
dnl		#include <ssl.h>
dnl		#endif
dnl

AC_DEFUN([AX_CHECK_SSL],
[
	SSL_CFLAGS="";
	SSL_LDFLAGS="";
	SSL_LIBS="";
	AC_ARG_WITH([ssldir],
		[
			AS_HELP_STRING([--with-ssldir'[=DIR]'],
			   [specify (open)ssl directory @<:@default=check@:>@])
		],
		[],
		[with_ssldir=check])
	AS_IF([test "x$with_ssldir" != "xno"],
	[
		AC_MSG_CHECKING([for ssl])
		ssl_searchlist="";
		ssl_found="no";
		AS_IF([test "x$with_ssldir" == "xyes" -o \
		    "x$with_ssldir" == "xcheck"],
		[
			dnl use default search directories
			ssl_searchlist="$ssl_searchlist /usr/local/ssl";
			ssl_searchlist="$ssl_searchlist /usr/lib/ssl";
			ssl_searchlist="$ssl_searchlist /usr/ssl";
			ssl_searchlist="$ssl_searchlist /usr/pkg";
			ssl_searchlist="$ssl_searchlist /usr/local";
			ssl_searchlist="$ssl_searchlist /usr";
		], [
			ssl_searchlist="$with_ssldir";
		])
		for ssl_testdir in $ssl_searchlist; do
			AS_IF([test -f "$ssl_testdir/include/openssl/ssl.h"],
			[
				SSL_CFLAGS="-I$ssl_testdir/include/openssl/";
				SSL_LDFLAGS="-L$ssl_testdir/lib/";
				ssl_found="yes";
				break;
			])
			AS_IF([test -f "$ssl_testdir/include/ssl.h"],
			[
				SSL_CFLAGS="-I$ssl_testdir/include/";
				SSL_LDFLAGS="-L$ssl_testdir/lib/";
				ssl_found="yes";
				break;
			])
		done;
		AS_IF([test "x$ssl_found" == "xyes"],
		[
			AC_MSG_RESULT(yes)
			AC_DEFINE([HAVE_SSL], [1], [Define if ssl exists.])
			SSL_LIBS="-lssl -lcrypto";
		], [
			AC_MSG_RESULT(no)
			AC_MSG_ERROR([Please install ssl library : libssl-dev])
		])
	])
	dnl substitute variables in any cases
	AC_SUBST(SSL_CFLAGS)
	AC_SUBST(SSL_LDFLAGS)
	AC_SUBST(SSL_LIBS)
])dnl
