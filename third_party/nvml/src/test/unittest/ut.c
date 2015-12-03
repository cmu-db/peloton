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
 * ut.c -- unit test support routines
 *
 * some of these functions look at errno, but none of them
 * change errno -- it is preserved across these calls.
 *
 * ut_done() and ut_fatal() never return.
 */

#include "unittest.h"

/* RHEL5 seems to be missing decls, even though libc supports them */
extern DIR *fdopendir(int fd);
extern ssize_t readlinkat(int, const char *restrict, char *restrict, size_t);

#define	MAXLOGNAME 100		/* maximum expected .log file name length */
#define	MAXPRINT 8192		/* maximum expected single print length */

/*
 * output gets replicated to these files
 */
static FILE *Outfp;
static FILE *Errfp;
static FILE *Tracefp;

static int Quiet;		/* set by UNITTEST_QUIET env variable */
static char *Testname;		/* set by UNITTEST_NAME env variable */
unsigned long Ut_pagesize;

/*
 * flags that control output
 */
#define	OF_NONL		1	/* do not append newline */
#define	OF_ERR		2	/* output is error output */
#define	OF_TRACE	4	/* output to trace file only */
#define	OF_LOUD		8	/* output even in Quiet mode */
#define	OF_NAME		16	/* include Testname in the output */

/*
 * vout -- common output code, all output happens here
 */
static void
vout(int flags, const char *prepend, const char *fmt, va_list ap)
{
	char buf[MAXPRINT];
	unsigned cc = 0;
	int sn;
	int quiet = Quiet;
	const char *sep = "";
	const char *errstr = "";
	const char *nl = "\n";

	if (flags & OF_LOUD)
		quiet = 0;

	if (flags & OF_NONL)
		nl = "";

	if (flags & OF_NAME && Testname) {
		sn = snprintf(&buf[cc], MAXPRINT - cc, "%s: ", Testname);
		if (sn < 0)
			abort();
		cc += (unsigned)sn;
	}

	if (prepend) {
		const char *colon = "";

		if (fmt)
			colon = ": ";

		sn = snprintf(&buf[cc], MAXPRINT - cc, "%s%s", prepend, colon);
		if (sn < 0)
			abort();
		cc += (unsigned)sn;
	}

	if (fmt) {
		if (*fmt == '!') {
			fmt++;
			sep = ": ";
			errstr = strerror(errno);
		}
		sn = vsnprintf(&buf[cc], MAXPRINT - cc, fmt, ap);
		if (sn < 0)
			abort();
		cc += (unsigned)sn;
	}

	snprintf(&buf[cc], MAXPRINT - cc, "%s%s%s", sep, errstr, nl);

	/* buf has the fully-baked output, send it everywhere it goes... */
	fputs(buf, Tracefp);
	if (flags & OF_ERR) {
		fputs(buf, Errfp);
		if (!quiet)
			fputs(buf, stderr);
	} else if ((flags & OF_TRACE) == 0) {
		fputs(buf, Outfp);
		if (!quiet)
			fputs(buf, stdout);
	}
}

/*
 * out -- printf-like output controlled by flags
 */
static void
out(int flags, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);

	vout(flags, NULL, fmt, ap);

	va_end(ap);
}

/*
 * prefix -- emit the trace line prefix
 */
static void
prefix(const char *file, int line, const char *func)
{
	out(OF_NONL|OF_TRACE, "{%s:%d %s} ", file, line, func);
}

/*
 * lookup table for open files
 */
static struct fd_lut {
	struct fd_lut *left;
	struct fd_lut *right;
	int fdnum;
	char *fdfile;
} *Fd_lut;

static int Fd_errcount;

/*
 * open_file_add -- add an open file to the lut
 */
static struct fd_lut *
open_file_add(struct fd_lut *root, int fdnum, const char *fdfile)
{
	if (root == NULL) {
		root = ZALLOC(sizeof (*root));
		root->fdnum = fdnum;
		root->fdfile = STRDUP(fdfile);
	} else if (root->fdnum == fdnum)
		FATAL("duplicate fdnum: %d", fdnum);
	else if (root->fdnum < fdnum)
		root->left = open_file_add(root->left, fdnum, fdfile);
	else
		root->right = open_file_add(root->right, fdnum, fdfile);
	return root;
}

/*
 * open_file_remove -- find exact match & remove it from lut
 *
 * prints error if exact match not found, increments Fd_errcount
 */
static void
open_file_remove(struct fd_lut *root, int fdnum, const char *fdfile)
{
	if (root == NULL) {
		ERR("unexpected open file: fd %d => \"%s\"", fdnum, fdfile);
		Fd_errcount++;
	} else if (root->fdnum == fdnum) {
		if (root->fdfile == NULL) {
			ERR("open file dup: fd %d => \"%s\"", fdnum, fdfile);
			Fd_errcount++;
		} else if (strcmp(root->fdfile, fdfile) == 0) {
			/* found exact match */
			FREE(root->fdfile);
			root->fdfile = NULL;
		} else {
			ERR("open file changed: fd %d was \"%s\" now \"%s\"",
			    fdnum, root->fdfile, fdfile);
			Fd_errcount++;
		}
	} else if (root->fdnum < fdnum)
		open_file_remove(root->left, fdnum, fdfile);
	else
		open_file_remove(root->right, fdnum, fdfile);
}

/*
 * open_file_walk -- walk lut for any left-overs
 *
 * prints error if any found, increments Fd_errcount
 */
static void
open_file_walk(struct fd_lut *root)
{
	if (root) {
		open_file_walk(root->left);
		if (root->fdfile) {
			ERR("open file missing: fd %d => \"%s\"",
			    root->fdnum, root->fdfile);
			Fd_errcount++;
		}
		open_file_walk(root->right);
	}
}

/*
 * open_file_free -- free the lut
 */
static void
open_file_free(struct fd_lut *root)
{
	if (root) {
		open_file_free(root->left);
		open_file_free(root->right);
		if (root->fdfile)
			FREE(root->fdfile);
		FREE(root);
	}
}

/*
 * record_open_files -- make a list of open files (used at START() time)
 */
static void
record_open_files()
{
	int dirfd;
	DIR *dirp = NULL;
	struct dirent *dp;

	if ((dirfd = open("/proc/self/fd", O_RDONLY)) < 0 ||
	    (dirp = fdopendir(dirfd)) == NULL)
		FATAL("!/proc/self/fd");
	while ((dp = readdir(dirp)) != NULL) {
		int fdnum;
		char fdfile[PATH_MAX];
		ssize_t cc;

		if (*dp->d_name == '.')
			continue;
		if ((cc = readlinkat(dirfd, dp->d_name, fdfile, PATH_MAX)) < 0)
		    FATAL("!readlinkat: /proc/self/fd/%s", dp->d_name);
		fdfile[cc] = '\0';
		fdnum = atoi(dp->d_name);
		if (dirfd == fdnum)
			continue;
		Fd_lut = open_file_add(Fd_lut, fdnum, fdfile);
	}
	closedir(dirp);
}

/*
 * check_open_files -- verify open files match recorded open files
 */
static void
check_open_files()
{
	int dirfd;
	DIR *dirp = NULL;
	struct dirent *dp;

	if ((dirfd = open("/proc/self/fd", O_RDONLY)) < 0 ||
	    (dirp = fdopendir(dirfd)) == NULL)
		FATAL("!/proc/self/fd");
	while ((dp = readdir(dirp)) != NULL) {
		int fdnum;
		char fdfile[PATH_MAX];
		ssize_t cc;

		if (*dp->d_name == '.')
			continue;
		if ((cc = readlinkat(dirfd, dp->d_name, fdfile, PATH_MAX)) < 0)
		    FATAL("!readlinkat: /proc/self/fd/%s", dp->d_name);
		fdfile[cc] = '\0';
		fdnum = atoi(dp->d_name);
		if (dirfd == fdnum)
			continue;
		open_file_remove(Fd_lut, fdnum, fdfile);
	}
	closedir(dirp);
	open_file_walk(Fd_lut);
	if (Fd_errcount)
		FATAL("open file list changed between START() and DONE()");
	open_file_free(Fd_lut);
}

/*
 * ut_start -- initialize unit test framework, indicate test started
 */
void
ut_start(const char *file, int line, const char *func,
    int argc, char * const argv[], const char *fmt, ...)
{
	va_list ap;
	int saveerrno = errno;
	char logname[MAXLOGNAME];
	char *logsuffix;

	va_start(ap, fmt);

	if (getenv("UNITTEST_NO_SIGHANDLERS") == NULL)
		ut_register_sighandlers();

	if (getenv("UNITTEST_QUIET") != NULL)
		Quiet++;

	Testname = getenv("UNITTEST_NAME");

	if ((logsuffix = getenv("UNITTEST_NUM")) == NULL)
		logsuffix = "";

	snprintf(logname, MAXLOGNAME, "out%s.log", logsuffix);
	if ((Outfp = fopen(logname, "w")) == NULL) {
		perror(logname);
		exit(1);
	}

	snprintf(logname, MAXLOGNAME, "err%s.log", logsuffix);
	if ((Errfp = fopen(logname, "w")) == NULL) {
		perror(logname);
		exit(1);
	}

	snprintf(logname, MAXLOGNAME, "trace%s.log", logsuffix);
	if ((Tracefp = fopen(logname, "w")) == NULL) {
		perror(logname);
		exit(1);
	}

	setlinebuf(Outfp);
	setlinebuf(Errfp);
	setlinebuf(Tracefp);
	setlinebuf(stdout);

	prefix(file, line, func);
	vout(OF_LOUD|OF_NAME, "START", fmt, ap);

	out(OF_NONL, 0, "     args:");
	for (int i = 0; i < argc; i++)
		out(OF_NONL, " %s", argv[i]);
	out(0, NULL);

	va_end(ap);

	/* generate a uuid so the leaked fd gets recorded */
	uuid_t u;
	uuid_generate(u);

	record_open_files();

	long long sc = sysconf(_SC_PAGESIZE);
	if (sc < 0)
		abort();
	Ut_pagesize = (unsigned long)sc;

	errno = saveerrno;
}

/*
 * ut_done -- indicate test is done, exit program
 */
void
ut_done(const char *file, int line, const char *func,
    const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);

	check_open_files();

	prefix(file, line, func);
	vout(OF_NAME, "Done", fmt, ap);

	va_end(ap);

	if (Outfp != NULL)
		fclose(Outfp);

	if (Errfp != NULL)
		fclose(Errfp);

	if (Tracefp != NULL)
		fclose(Tracefp);

	exit(0);
}

/*
 * ut_fatal -- indicate fatal error, exit program
 */
void
ut_fatal(const char *file, int line, const char *func,
    const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);

	prefix(file, line, func);
	vout(OF_ERR|OF_NAME, "Error", fmt, ap);

	va_end(ap);

	abort();
}

/*
 * ut_out -- output to stdout
 */
void
ut_out(const char *file, int line, const char *func,
    const char *fmt, ...)
{
	va_list ap;
	int saveerrno = errno;

	va_start(ap, fmt);

	prefix(file, line, func);
	vout(0, NULL, fmt, ap);

	va_end(ap);

	errno = saveerrno;
}

/*
 * ut_err -- output to stderr
 */
void
ut_err(const char *file, int line, const char *func,
    const char *fmt, ...)
{
	va_list ap;
	int saveerrno = errno;

	va_start(ap, fmt);

	prefix(file, line, func);
	vout(OF_ERR|OF_NAME, NULL, fmt, ap);

	va_end(ap);

	errno = saveerrno;
}

/*
 * ut_checksum -- compute checksum using Fletcher16 algorithm
 */
uint16_t
ut_checksum(uint8_t *addr, size_t len)
{
	uint16_t sum1 = 0;
	uint16_t sum2 = 0;

	for (size_t i = 0; i < len; ++i) {
		sum1 = (sum1 + addr[i]) % 255;
		sum2 = (sum2 + sum1) % 255;
	}

	return (sum2 << 8) | sum1;
}
