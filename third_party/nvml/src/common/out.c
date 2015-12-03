/*
 * Copyright (c) 2014-2015, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * out.c -- support for logging, tracing, and assertion output
 *
 * Macros like LOG(), OUT, ASSERT(), etc. end up here.
 */

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>

#include "out.h"
#include "valgrind_internal.h"

static char nvml_src_version[] = "SRCVERSION:" SRCVERSION;

static const char *Log_prefix;
static int Log_level;
static FILE *Out_fp;

#ifndef NO_LIBPTHREAD

#define	MAXPRINT 8192	/* maximum expected log line */

static pthread_once_t Last_errormsg_key_once = PTHREAD_ONCE_INIT;
static pthread_key_t Last_errormsg_key;

static void
_Last_errormsg_key_alloc()
{
	int pth_ret = pthread_key_create(&Last_errormsg_key, free);
	if (pth_ret)
		FATAL("!pthread_key_create");

	VALGRIND_ANNOTATE_HAPPENS_BEFORE(&Last_errormsg_key_once);
}

static void
Last_errormsg_key_alloc()
{
	pthread_once(&Last_errormsg_key_once, _Last_errormsg_key_alloc);
	/*
	 * Workaround Helgrind's bug:
	 * https://bugs.kde.org/show_bug.cgi?id=337735
	 */
	VALGRIND_ANNOTATE_HAPPENS_AFTER(&Last_errormsg_key_once);
}

static inline void
Last_errormsg_fini()
{
	void *p = pthread_getspecific(Last_errormsg_key);
	if (p) {
		free(p);
		(void) pthread_setspecific(Last_errormsg_key, NULL);
	}
}

static inline const char *
Last_errormsg_get()
{
	Last_errormsg_key_alloc();

	char *errormsg = pthread_getspecific(Last_errormsg_key);
	if (errormsg == NULL) {
		errormsg = malloc(MAXPRINT);
		int ret = pthread_setspecific(Last_errormsg_key, errormsg);
		if (ret)
			FATAL("!pthread_setspecific");
	}
	return errormsg;
}
#else

/*
 * We don't want libpmem to depend on libpthread.  Instead of using pthread
 * API to dynamically allocate thread-specific error message buffer, we put
 * it into TLS.  However, keeping a pretty large static buffer (8K) in TLS
 * may lead to some issues, so the maximum message length is reduced.
 * Fortunately, it looks like the longest error message in libpmem should
 * not be longer than about 90 chars (in case of pmem_check_version()).
 */

#define	MAXPRINT 256	/* maximum expected log line for libpmem */

static __thread char Last_errormsg[MAXPRINT];

static inline void
Last_errormsg_key_alloc()
{
}

static inline void
Last_errormsg_fini()
{
}

static inline const char *
Last_errormsg_get()
{
	return Last_errormsg;
}

#endif /* NO_LIBPTHREAD */

#ifdef	DEBUG
/*
 * getexecname -- return name of current executable
 *
 * This function is only used when logging is enabled, to make
 * it more clear in the log which program was running.
 */
static const char *
getexecname(void)
{
	char procpath[PATH_MAX];
	static char namepath[PATH_MAX];
	ssize_t cc;

	snprintf(procpath, PATH_MAX, "/proc/%d/exe", getpid());

	if ((cc = readlink(procpath, namepath, PATH_MAX)) < 0)
		strcpy(namepath, "unknown");
	else
		namepath[cc] = '\0';

	return namepath;
}
#endif	/* DEBUG */

/*
 * out_init -- initialize the log
 *
 * This is called from the library initialization code.
 */
void
out_init(const char *log_prefix, const char *log_level_var,
		const char *log_file_var, int major_version,
		int minor_version)
{
	static int once;

	/* only need to initialize the out module once */
	if (once)
		return;
	once++;

	Log_prefix = log_prefix;

#ifdef	DEBUG
	char *log_level;
	char *log_file;

	if ((log_level = getenv(log_level_var)) != NULL) {
		Log_level = atoi(log_level);
		if (Log_level < 0) {
			Log_level = 0;
		}
	}

	if ((log_file = getenv(log_file_var)) != NULL) {
		size_t cc = strlen(log_file);

		/* reserve more than enough space for a PID + '\0' */
		char log_file_pid[cc + 30];

		if (cc > 0 && log_file[cc - 1] == '-') {
			snprintf(log_file_pid, cc + 30, "%s%d",
				log_file, getpid());
			log_file = log_file_pid;
		}
		if ((Out_fp = fopen(log_file, "w")) == NULL) {
			fprintf(stderr, "Error (%s): %s=%s: %s\n",
					log_prefix, log_file_var,
					log_file, strerror(errno));
			abort();
		}
	}
#endif	/* DEBUG */

	if (Out_fp == NULL)
		Out_fp = stderr;
	else
		setlinebuf(Out_fp);

#ifdef	DEBUG
	LOG(1, "pid %d: program: %s", getpid(), getexecname());
#endif
	LOG(1, "%s version %d.%d", log_prefix, major_version, minor_version);
	LOG(1, "src version %s", nvml_src_version);
#ifdef USE_VG_PMEMCHECK
	/*
	 * Attribute "used" to prevent compiler from optimizing out the variable
	 * when LOG expands to no code (!DEBUG)
	 */
	static __attribute__((used)) const char *pmemcheck_msg =
			"compiled with support for Valgrind pmemcheck";
	LOG(1, "%s", pmemcheck_msg);
#endif /* USE_VG_PMEMCHECK */
#ifdef USE_VG_HELGRIND
	static __attribute__((used)) const char *helgrind_msg =
			"compiled with support for Valgrind helgrind";
	LOG(1, "%s", helgrind_msg);
#endif /* USE_VG_HELGRIND */
#ifdef USE_VG_MEMCHECK
	static __attribute__((used)) const char *memcheck_msg =
			"compiled with support for Valgrind memcheck";
	LOG(1, "%s", memcheck_msg);
#endif /* USE_VG_MEMCHECK */

	Last_errormsg_key_alloc();
}

/*
 * out_fini -- close the log file
 *
 * This is called to close log file before process stop.
 */
void
out_fini()
{
	if (Out_fp != NULL && Out_fp != stderr) {
		fclose(Out_fp);
		Out_fp = stderr;
	}

	Last_errormsg_fini();
}

/*
 * out_print_func -- default print_func, goes to stderr or Out_fp
 */
static void
out_print_func(const char *s)
{
	fputs(s, Out_fp);
}

/*
 * calling Print(s) calls the current print_func...
 */
typedef void (*Print_func)(const char *s);
typedef int (*Vsnprintf_func)(char *str, size_t size, const char *format,
		va_list ap);
static Print_func Print = out_print_func;
static Vsnprintf_func Vsnprintf = vsnprintf;

/*
 * out_set_print_func -- allow override of print_func used by out module
 */
void
out_set_print_func(void (*print_func)(const char *s))
{
	LOG(3, "print %p", print_func);

	Print = (print_func == NULL) ? out_print_func : print_func;
}

/*
 * out_set_vsnprintf_func -- allow override of vsnprintf_func used by out module
 */
void
out_set_vsnprintf_func(int (*vsnprintf_func)(char *str, size_t size,
				const char *format, va_list ap))
{
	LOG(3, "vsnprintf %p", vsnprintf_func);

	Vsnprintf = (vsnprintf_func == NULL) ? vsnprintf : vsnprintf_func;
}

/*
 * out_snprintf -- (internal) custom snprintf implementation
 */
__attribute__((format(printf, 3, 4)))
static int
out_snprintf(char *str, size_t size, const char *format, ...)
{
	int ret;
	va_list ap;

	va_start(ap, format);
	ret = Vsnprintf(str, size, format, ap);
	va_end(ap);

	return (ret);
}

/*
 * out_common -- common output code, all output goes through here
 */
static void
out_common(const char *file, int line, const char *func, int level,
		const char *suffix, const char *fmt, va_list ap)
{
	int oerrno = errno;
	char buf[MAXPRINT];
	unsigned cc = 0;
	int ret;
	const char *sep = "";
	const char *errstr = "";

	if (file) {
		ret = out_snprintf(&buf[cc], MAXPRINT - cc,
				"<%s>: <%d> [%s:%d %s] ",
				Log_prefix, level, file, line, func);
		if (ret < 0) {
			Print("out_snprintf failed");
			goto end;
		}
		cc += (unsigned)ret;
	}

	if (fmt) {
		if (*fmt == '!') {
			fmt++;
			sep = ": ";
			errstr = strerror(errno);
		}
		ret = Vsnprintf(&buf[cc], MAXPRINT - cc, fmt, ap);
		if (ret < 0) {
			Print("Vsnprintf failed");
			goto end;
		}
		cc += (unsigned)ret;
	}

	out_snprintf(&buf[cc], MAXPRINT - cc, "%s%s%s", sep, errstr, suffix);

	Print(buf);

end:
	errno = oerrno;
}

/*
 * out_error -- common error output code, all error messages go through here
 */
static void
out_error(const char *file, int line, const char *func,
		const char *suffix, const char *fmt, va_list ap)
{
	int oerrno = errno;
	unsigned cc = 0;
	int ret;
	const char *sep = "";
	const char *errstr = "";

	char *errormsg = (char *)out_get_errormsg();

	if (fmt) {
		if (*fmt == '!') {
			fmt++;
			sep = ": ";
			errstr = strerror(errno);
		}
		ret = Vsnprintf(&errormsg[cc], MAXPRINT, fmt, ap);
		if (ret < 0) {
			strcpy(errormsg, "Vsnprintf failed");
			goto end;
		}
		cc += (unsigned)ret;
		out_snprintf(&errormsg[cc], MAXPRINT - cc, "%s%s",
				sep, errstr);
	}

#ifdef DEBUG
	if (Log_level >= 1) {
		char buf[MAXPRINT];
		cc = 0;

		if (file) {
			ret = out_snprintf(&buf[cc], MAXPRINT,
					"<%s>: <1> [%s:%d %s] ",
					Log_prefix, file, line, func);
			if (ret < 0) {
				Print("out_snprintf failed");
				goto end;
			}
			cc += (unsigned)ret;
		}

		out_snprintf(&buf[cc], MAXPRINT - cc, "%s%s", errormsg,
				suffix);

		Print(buf);
	}
#endif

end:
	errno = oerrno;
}

/*
 * out -- output a line, newline added automatically
 */
void
out(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);

	out_common(NULL, 0, NULL, 0, "\n", fmt, ap);

	va_end(ap);
}

/*
 * out_nonl -- output a line, no newline added automatically
 */
void
out_nonl(int level, const char *fmt, ...)
{
	va_list ap;

	if (Log_level < level)
			return;

	va_start(ap, fmt);
	out_common(NULL, 0, NULL, level, "", fmt, ap);

	va_end(ap);
}

/*
 * out_log -- output a log line if Log_level >= level
 */
void
out_log(const char *file, int line, const char *func, int level,
		const char *fmt, ...)
{
	va_list ap;

	if (Log_level < level)
			return;

	va_start(ap, fmt);
	out_common(file, line, func, level, "\n", fmt, ap);

	va_end(ap);
}

/*
 * out_fatal -- output a fatal error & die (i.e. assertion failure)
 */
void
out_fatal(const char *file, int line, const char *func,
		const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);

	out_common(file, line, func, 1, "\n", fmt, ap);

	va_end(ap);

	abort();
}

/*
 * out_err -- output an error message
 */
void
out_err(const char *file, int line, const char *func,
		const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);

	out_error(file, line, func, "\n", fmt, ap);

	va_end(ap);
}

/*
 * out_get_errormsg -- get the last error message
 */
const char *
out_get_errormsg(void)
{
	return Last_errormsg_get();
}
