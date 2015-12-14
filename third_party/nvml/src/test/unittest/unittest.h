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
 * unittest.h -- the mundane stuff shared by all unit tests
 *
 * we want unit tests to be very thorough and check absolutely everything
 * in order to nail down the test case as precisely as possible and flag
 * anything at all unexpected.  as a result, most unit tests are 90% code
 * checking stuff that isn't really interesting to what is being tested.
 * to help address this, the macros defined here include all the boilerplate
 * error checking which prints information and exits on unexpected errors.
 *
 * the result changes this code:
 *
 *	if ((buf = malloc(size)) == NULL) {
 *		fprintf(stderr, "cannot allocate %d bytes for buf\n", size);
 *		exit(1);
 *	}
 *
 * into this code:
 *
 *	buf = MALLOC(size);
 *
 * and the error message includes the calling context information (file:line).
 * in general, using the all-caps version of a call means you're using the
 * unittest.h version which does the most common checking for you.  so
 * calling VMEM_CREATE() instead of vmem_create() returns the same
 * thing, but can never return an error since the unit test library checks for
 * it.  * for routines like vmem_delete() there is no corresponding
 * VMEM_DELETE() because there's no error to check for.
 *
 * all unit tests should use the same initialization:
 *
 *	START(argc, argv, "brief test description", ...);
 *
 * all unit tests should use these exit calls:
 *
 *	DONE("message", ...);
 *	FATAL("message", ...);
 *
 * uniform stderr and stdout messages:
 *
 *	OUT("message", ...);
 *	ERR("message", ...);
 *
 * in all cases above, the message is printf-like, taking variable args.
 * the message can be NULL.  it can start with "!" in which case the "!" is
 * skipped and the message gets the errno string appended to it, like this:
 *
 *	if (somesyscall(..) < 0)
 *		FATAL("!my message");
 */

#ifndef	_UNITTEST_H
#define	_UNITTEST_H 1

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <setjmp.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <uuid/uuid.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <dirent.h>
#include <pthread.h>

#include <libpmem.h>
#include <libpmemblk.h>
#include <libpmemlog.h>
#include <libpmemobj.h>
#include <libvmem.h>


/*
 * unit test support...
 */
void ut_start(const char *file, int line, const char *func,
	int argc, char * const argv[], const char *fmt, ...)
	__attribute__((format(printf, 6, 7)));
void ut_done(const char *file, int line, const char *func,
	const char *fmt, ...)
	__attribute__((format(printf, 4, 5)))
	__attribute__((noreturn));
void ut_fatal(const char *file, int line, const char *func,
	const char *fmt, ...)
	__attribute__((format(printf, 4, 5)))
	__attribute__((noreturn));
void ut_out(const char *file, int line, const char *func,
	const char *fmt, ...)
	__attribute__((format(printf, 4, 5)));
void ut_err(const char *file, int line, const char *func,
	const char *fmt, ...)
	__attribute__((format(printf, 4, 5)));

/* indicate the start of the test */
#define	START(argc, argv, ...)\
    ut_start(__FILE__, __LINE__, __func__, argc, argv, __VA_ARGS__)

/* normal exit from test */
#define	DONE(...)\
    ut_done(__FILE__, __LINE__, __func__, __VA_ARGS__)

/* fatal error detected */
#define	FATAL(...)\
    ut_fatal(__FILE__, __LINE__, __func__, __VA_ARGS__)

/* normal output */
#define	OUT(...)\
    ut_out(__FILE__, __LINE__, __func__, __VA_ARGS__)

/* error output */
#define	ERR(...)\
    ut_err(__FILE__, __LINE__, __func__, __VA_ARGS__)


/*
 * assertions...
 */

/* assert a condition is true */
#define	ASSERT(cnd)\
	((void)((cnd) || (ut_fatal(__FILE__, __LINE__, __func__,\
	"assertion failure: %s", #cnd), 0)))

/* assertion with extra info printed if assertion fails */
#define	ASSERTinfo(cnd, info) \
	((void)((cnd) || (ut_fatal(__FILE__, __LINE__, __func__,\
	"assertion failure: %s (%s = %s)", #cnd, #info, info), 0)))

/* assert two integer values are equal */
#define	ASSERTeq(lhs, rhs)\
	((void)(((lhs) == (rhs)) || (ut_fatal(__FILE__, __LINE__, __func__,\
	"assertion failure: %s (0x%llx) == %s (0x%llx)", #lhs,\
	(unsigned long long)(lhs), #rhs, (unsigned long long)(rhs)), 0)))

/* assert two integer values are not equal */
#define	ASSERTne(lhs, rhs)\
	((void)(((lhs) != (rhs)) || (ut_fatal(__FILE__, __LINE__, __func__,\
	"assertion failure: %s (0x%llx) != %s (0x%llx)", #lhs,\
	(unsigned long long)(lhs), #rhs, (unsigned long long)(rhs)), 0)))

/* assert pointer is fits range of [start, start + size) */
#define	ASSERTrange(ptr, start, size)\
	((void)(((uintptr_t)(ptr) >= (uintptr_t)(start) &&\
	(uintptr_t)(ptr) < (uintptr_t)(start) + (uintptr_t)(size)) ||\
	(ut_fatal(__FILE__, __LINE__, __func__,\
	"assert failure: %s (%p) is outside range [%s (%p), %s (%p))", #ptr,\
	(void *)(ptr), #start, (void *)(start), #start"+"#size,\
	(void *)((uintptr_t)(start) + (uintptr_t)(size))), 0)))

/*
 * memory allocation...
 */
void *ut_malloc(const char *file, int line, const char *func, size_t size);
void *ut_calloc(const char *file, int line, const char *func,
    size_t nmemb, size_t size);
void ut_free(const char *file, int line, const char *func, void *ptr);
void *ut_realloc(const char *file, int line, const char *func,
    void *ptr, size_t size);
char *ut_strdup(const char *file, int line, const char *func,
    const char *str);
void *ut_pagealignmalloc(const char *file, int line, const char *func,
    size_t size);
void *ut_memalign(const char *file, int line, const char *func,
    size_t alignment, size_t size);
void *ut_mmap_anon_aligned(const char *file, int line, const char *func,
    size_t alignment, size_t size);
int ut_munmap_anon_aligned(const char *file, int line, const char *func,
    void *start, size_t size);

/* a malloc() that can't return NULL */
#define	MALLOC(size)\
    ut_malloc(__FILE__, __LINE__, __func__, size)

/* a calloc() that can't return NULL */
#define	CALLOC(nmemb, size)\
    ut_calloc(__FILE__, __LINE__, __func__, nmemb, size)

/* a malloc() of zeroed memory */
#define	ZALLOC(size)\
    ut_calloc(__FILE__, __LINE__, __func__, 1, size)

#define	FREE(ptr)\
    ut_free(__FILE__, __LINE__, __func__, ptr)

/* a realloc() that can't return NULL */
#define	REALLOC(ptr, size)\
    ut_realloc(__FILE__, __LINE__, __func__, ptr, size)

/* a strdup() that can't return NULL */
#define	STRDUP(str)\
    ut_strdup(__FILE__, __LINE__, __func__, str)

/* a malloc() that only returns page aligned memory */
#define	PAGEALIGNMALLOC(size)\
    ut_pagealignmalloc(__FILE__, __LINE__, __func__, size)

/* a malloc() that returns memory with given alignment */
#define	MEMALIGN(alignment, size)\
    ut_memalign(__FILE__, __LINE__, __func__, alignment, size)

/*
 * A mmap() that returns anonymous memory with given alignment and guard
 * pages.
 */
#define	MMAP_ANON_ALIGNED(size, alignment)\
    ut_mmap_anon_aligned(__FILE__, __LINE__, __func__, alignment, size)

#define	MUNMAP_ANON_ALIGNED(start, size)\
    ut_munmap_anon_aligned(__FILE__, __LINE__, __func__, start, size)

/*
 * file operations
 */
int ut_open(const char *file, int line, const char *func, const char *path,
    int flags, ...);

int ut_close(const char *file, int line, const char *func, int fd);

int ut_unlink(const char *file, int line, const char *func, const char *path);

int ut_access(const char *file, int line, const char *func, const char *path,
    int mode);

size_t ut_write(const char *file, int line, const char *func, int fd,
    const void *buf, size_t len);

size_t ut_read(const char *file, int line, const char *func, int fd,
    void *buf, size_t len);

size_t ut_readlink(const char *file, int line, const char *func,
    const char *path, void *buf, size_t len);

int ut_fcntl(const char *file, int line, const char *func, int fd,
    int cmd, int num, ...);

off_t ut_lseek(const char *file, int line, const char *func, int fd,
    off_t offset, int whence);

int ut_posix_fallocate(const char *file, int line, const char *func, int fd,
    off_t offset, off_t len);

int ut_stat(const char *file, int line, const char *func, const char *path,
    struct stat *st_bufp);

int ut_fstat(const char *file, int line, const char *func, int fd,
    struct stat *st_bufp);

int ut_flock(const char *file, int line, const char *func, int fd, int op);

void *ut_mmap(const char *file, int line, const char *func, void *addr,
    size_t length, int prot, int flags, int fd, off_t offset);

int ut_munmap(const char *file, int line, const char *func, void *addr,
    size_t length);

int ut_mprotect(const char *file, int line, const char *func, void *addr,
    size_t len, int prot);

int ut_symlink(const char *file, int line, const char *func,
    const char *oldpath, const char *newpath);

int ut_link(const char *file, int line, const char *func,
    const char *oldpath, const char *newpath);

int ut_mkdir(const char *file, int line, const char *func,
    const char *pathname, mode_t mode);

int ut_rmdir(const char *file, int line, const char *func,
    const char *pathname);

int ut_rename(const char *file, int line, const char *func,
    const char *oldpath, const char *newpath);

int ut_mount(const char *file, int line, const char *func, const char *src,
    const char *tar, const char *fstype, unsigned long flags,
    const void *data);

int ut_umount(const char *file, int line, const char *func, const char *tar);

int ut_pselect(const char *file, int line, const char *func, int nfds,
    fd_set *rfds, fd_set *wfds, fd_set *efds, const struct timespec *tv,
    const sigset_t *sigmask);

int ut_mknod(const char *file, int line, const char *func,
    const char *pathname, mode_t mode, dev_t dev);

int utstat(const char *file, int line, const char *func, const char *path,
    struct stat *st_bufp);

int ut_truncate(const char *file, int line, const char *func,
    const char *path, off_t length);

int ut_ftruncate(const char *file, int line, const char *func,
    int fd, off_t length);

int ut_chmod(const char *file, int line, const char *func,
    const char *path, mode_t mode);

DIR *ut_opendir(const char *file, int line, const char *func, const char *name);

int ut_dirfd(const char *file, int line, const char *func, DIR *dirp);

int ut_closedir(const char *file, int line, const char *func, DIR *dirp);

/* an open() that can't return < 0 */
#define	OPEN(path, ...)\
    ut_open(__FILE__, __LINE__, __func__, path, __VA_ARGS__)

/* a close() that can't return -1 */
#define	CLOSE(fd)\
    ut_close(__FILE__, __LINE__, __func__, fd)

/* an unlink() that can't return -1 */
#define	UNLINK(path)\
    ut_unlink(__FILE__, __LINE__, __func__, path)

/* an access() that can't return -1 */
#define	ACCESS(path, mode)\
    ut_access(__FILE__, __LINE__, __func__, path, mode)

/* a write() that can't return -1 */
#define	WRITE(fd, buf, len)\
    ut_write(__FILE__, __LINE__, __func__, fd, buf, len)

/* a read() that can't return -1 */
#define	READ(fd, buf, len)\
    ut_read(__FILE__, __LINE__, __func__, fd, buf, len)

/* a readlink() that can't return -1 */
#define	READLINK(path, buf, len)\
    ut_readlink(__FILE__, __LINE__, __func__, path, buf, len)

/* a lseek() that can't return -1 */
#define	LSEEK(fd, offset, whence)\
    ut_lseek(__FILE__, __LINE__, __func__, fd, offset, whence)
/*
 * The C Standard specifies that at least one argument must be passed to
 * the ellipsis, to ensure that the macro does not resolve to an expression
 * with a trailing comma. So, when calling this macro if num = 0
 * pass in a 0 for the argument.
 */
#define	FCNTL(fd, cmd, num, ...)\
	ut_fcntl(__FILE__, __LINE__, __func__, fd, cmd, num, __VA_ARGS__)

#define	POSIX_FALLOCATE(fd, off, len)\
    ut_posix_fallocate(__FILE__, __LINE__, __func__, fd, off, len)

#define	FSTAT(fd, st_bufp)\
    ut_fstat(__FILE__, __LINE__, __func__, fd, st_bufp)

#define	FLOCK(fd, op)\
    ut_flock(__FILE__, __LINE__, __func__, fd, op)

/* a mmap() that can't return MAP_FAILED */
#define	MMAP(addr, len, prot, flags, fd, offset)\
    ut_mmap(__FILE__, __LINE__, __func__, addr, len, prot, flags, fd, offset);

/* a munmap() that can't return -1 */
#define	MUNMAP(addr, length)\
    ut_munmap(__FILE__, __LINE__, __func__, addr, length);

/* a mprotect() that can't return -1 */
#define	MPROTECT(addr, len, prot)\
    ut_mprotect(__FILE__, __LINE__, __func__, addr, len, prot);

#define	STAT(path, st_bufp)\
    ut_stat(__FILE__, __LINE__, __func__, path, st_bufp)

#define	SYMLINK(oldpath, newpath)\
    ut_symlink(__FILE__, __LINE__, __func__, oldpath, newpath)

#define	LINK(oldpath, newpath)\
    ut_link(__FILE__, __LINE__, __func__, oldpath, newpath)

#define	MKDIR(pathname, mode)\
    ut_mkdir(__FILE__, __LINE__, __func__, pathname, mode)

#define	RMDIR(pathname)\
    ut_rmdir(__FILE__, __LINE__, __func__, pathname)

#define	RENAME(oldpath, newpath)\
    ut_rename(__FILE__, __LINE__, __func__, oldpath, newpath)

#define	MOUNT(src, tar, fstype, flags, data)\
    ut_mount(__FILE__, __LINE__, __func__, src, tar, fstype, flags, data)

#define	UMOUNT(tar)\
    ut_umount(__FILE__, __LINE__, __func__, tar)

#define	PSELECT(nfds, rfds, wfds, efds, tv, sigmask)\
    ut_pselect(__FILE__, __LINE__, __func__, nfds, rfds, wfds, efds,\
	tv, sigmask)

#define	MKNOD(pathname, mode, dev)\
    ut_mknod(__FILE__, __LINE__, __func__, pathname, mode, dev)

#define	TRUNCATE(path, length)\
    ut_truncate(__FILE__, __LINE__, __func__, path, length)

#define	FTRUNCATE(fd, length)\
    ut_ftruncate(__FILE__, __LINE__, __func__, fd, length)

#define	CHMOD(path, mode)\
    ut_chmod(__FILE__, __LINE__, __func__, path, mode)

#define	OPENDIR(name)\
    ut_opendir(__FILE__, __LINE__, __func__, name)

#define	DIRFD(dirp)\
    ut_dirfd(__FILE__, __LINE__, __func__, dirp)

#define	CLOSEDIR(dirp)\
    ut_closedir(__FILE__, __LINE__, __func__, dirp)

/*
 * signals...
 */
int ut_sigaction(const char *file, int line, const char *func,
		int signum, struct sigaction *act, struct sigaction *oldact);

/* a sigaction() that can't return an error */
#define	SIGACTION(signum, act, oldact)\
    ut_sigaction(__FILE__, __LINE__, __func__, signum, act, oldact)

/*
 * pthreads...
 */
int ut_pthread_create(const char *file, int line, const char *func,
    pthread_t *restrict thread,
    const pthread_attr_t *restrict attr,
    void *(*start_routine)(void *), void *restrict arg);
int ut_pthread_join(const char *file, int line, const char *func,
    pthread_t thread, void **value_ptr);

/* a pthread_create() that can't return an error */
#define	PTHREAD_CREATE(thread, attr, start_routine, arg)\
    ut_pthread_create(__FILE__, __LINE__, __func__,\
    thread, attr, start_routine, arg)

/* a pthread_join() that can't return an error */
#define	PTHREAD_JOIN(thread, value_ptr)\
    ut_pthread_join(__FILE__, __LINE__, __func__, thread, value_ptr)

/*
 * mocks...
 */
#define	_FUNC_REAL_DECL(name, ret_type, ...)\
	ret_type __real_##name(__VA_ARGS__) __attribute__((unused));

#define	_FUNC_REAL(name)\
	__real_##name

#define	RCOUNTER(name)\
	_rcounter##name

#define	FUNC_MOCK_RCOUNTER_SET(name, val)\
    RCOUNTER(name) = val;

#define	FUNC_MOCK(name, ret_type, ...)\
	_FUNC_REAL_DECL(name, ret_type, ##__VA_ARGS__)\
	static int RCOUNTER(name);\
	ret_type __wrap_##name(__VA_ARGS__);\
	ret_type __wrap_##name(__VA_ARGS__) {\
		switch (__sync_fetch_and_add(&RCOUNTER(name), 1)) {

#define	FUNC_MOCK_END\
	}}

#define	FUNC_MOCK_RUN(run)\
	case run:

#define	FUNC_MOCK_RUN_DEFAULT\
	default:

#define	FUNC_MOCK_RUN_RET(run, ret)\
	case run: return (ret);

#define	FUNC_MOCK_RUN_RET_DEFAULT_REAL(name, ...)\
	default: return _FUNC_REAL(name)(__VA_ARGS__);

#define	FUNC_MOCK_RUN_RET_DEFAULT(ret)\
	default: return (ret);

#define	FUNC_MOCK_RET_ALWAYS(name, ret_type, ret, ...)\
	FUNC_MOCK(name, ret_type, __VA_ARGS__)\
		FUNC_MOCK_RUN_RET_DEFAULT(ret);\
	FUNC_MOCK_END

extern unsigned long Ut_pagesize;

void ut_dump_backtrace(void);
void ut_sighandler(int);
void ut_register_sighandlers(void);

uint16_t ut_checksum(uint8_t *addr, size_t len);

#endif	/* unittest.h */
