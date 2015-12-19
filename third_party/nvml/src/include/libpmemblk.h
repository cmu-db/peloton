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
 * libpmemblk.h -- definitions of libpmemblk entry points
 *
 * This library provides support for programming with persistent memory (pmem).
 *
 * libpmemblk provides support for arrays of atomically-writable blocks.
 *
 * See libpmemblk(3) for details.
 */

#ifndef	LIBPMEMBLK_H
#define	LIBPMEMBLK_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>

/*
 * opaque type, internal to libpmemblk
 */
typedef struct pmemblk PMEMblkpool;

/*
 * PMEMBLK_MAJOR_VERSION and PMEMBLK_MINOR_VERSION provide the current version
 * of the libpmemblk API as provided by this header file.  Applications can
 * verify that the version available at run-time is compatible with the version
 * used at compile-time by passing these defines to pmemblk_check_version().
 */
#define	PMEMBLK_MAJOR_VERSION 1
#define	PMEMBLK_MINOR_VERSION 0
const char *pmemblk_check_version(
		unsigned major_required,
		unsigned minor_required);

/* minimum pool size: 16MB + 8KB (minimum BTT size + header size) */
#define	PMEMBLK_MIN_POOL ((size_t)((1u << 20) * 16 + (1u << 10) * 8))

#define	PMEMBLK_MIN_BLK ((size_t)512)

PMEMblkpool *pmemblk_open(const char *path, size_t bsize);
PMEMblkpool *pmemblk_create(const char *path, size_t bsize,
		size_t poolsize, mode_t mode);
void pmemblk_close(PMEMblkpool *pbp);
int pmemblk_check(const char *path, size_t bsize);
size_t pmemblk_bsize(PMEMblkpool *pbp);
size_t pmemblk_nblock(PMEMblkpool *pbp);
int pmemblk_read(PMEMblkpool *pbp, void *buf, off_t blockno);
int pmemblk_write(PMEMblkpool *pbp, const void *buf, off_t blockno);
int pmemblk_set_zero(PMEMblkpool *pbp, off_t blockno);
int pmemblk_set_error(PMEMblkpool *pbp, off_t blockno);

/*
 * Passing NULL to pmemblk_set_funcs() tells libpmemblk to continue to use the
 * default for that function.  The replacement functions must not make calls
 * back into libpmemblk.
 */
void pmemblk_set_funcs(
		void *(*malloc_func)(size_t size),
		void (*free_func)(void *ptr),
		void *(*realloc_func)(void *ptr, size_t size),
		char *(*strdup_func)(const char *s));

const char *pmemblk_errormsg(void);

#ifdef __cplusplus
}
#endif
#endif	/* libpmemblk.h */
