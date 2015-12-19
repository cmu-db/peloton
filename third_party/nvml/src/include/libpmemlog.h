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
 * libpmemlog.h -- definitions of libpmemlog entry points
 *
 * This library provides support for programming with persistent memory (pmem).
 *
 * libpmemlog provides support for pmem-resident log files.
 *
 * See libpmemlog(3) for details.
 */

#ifndef	LIBPMEMLOG_H
#define	LIBPMEMLOG_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>
#include <sys/uio.h>

/*
 * opaque type, internal to libpmemlog
 */
typedef struct pmemlog PMEMlogpool;

/*
 * PMEMLOG_MAJOR_VERSION and PMEMLOG_MINOR_VERSION provide the current
 * version of the libpmemlog API as provided by this header file.
 * Applications can verify that the version available at run-time
 * is compatible with the version used at compile-time by passing
 * these defines to pmemlog_check_version().
 */
#define	PMEMLOG_MAJOR_VERSION 1
#define	PMEMLOG_MINOR_VERSION 0
const char *pmemlog_check_version(
		unsigned major_required,
		unsigned minor_required);

/*
 * support for PMEM-resident log files...
 */
#define	PMEMLOG_MIN_POOL ((size_t)(1024 * 1024 * 2)) /* min pool size: 2MB */

PMEMlogpool *pmemlog_open(const char *path);
PMEMlogpool *pmemlog_create(const char *path, size_t poolsize, mode_t mode);
void pmemlog_close(PMEMlogpool *plp);
int pmemlog_check(const char *path);
size_t pmemlog_nbyte(PMEMlogpool *plp);
int pmemlog_append(PMEMlogpool *plp, const void *buf, size_t count);
int pmemlog_appendv(PMEMlogpool *plp, const struct iovec *iov, int iovcnt);
off_t pmemlog_tell(PMEMlogpool *plp);
void pmemlog_rewind(PMEMlogpool *plp);
void pmemlog_walk(PMEMlogpool *plp, size_t chunksize,
	int (*process_chunk)(const void *buf, size_t len, void *arg),
	void *arg);

/*
 * Passing NULL to pmemlog_set_funcs() tells libpmemlog to continue to use the
 * default for that function.  The replacement functions must not make calls
 * back into libpmemlog.
 */
void pmemlog_set_funcs(
		void *(*malloc_func)(size_t size),
		void (*free_func)(void *ptr),
		void *(*realloc_func)(void *ptr, size_t size),
		char *(*strdup_func)(const char *s));

const char *pmemlog_errormsg(void);

#ifdef __cplusplus
}
#endif
#endif	/* libpmemlog.h */
