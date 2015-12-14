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
 * ut_file.c -- unit test file operations
 */

#include "unittest.h"

/*
 * ut_open -- an open that cannot return < 0
 */
int
ut_open(const char *file, int line, const char *func, const char *path,
    int flags, ...)
{
	va_list ap;
	int mode;

	va_start(ap, flags);
	mode = va_arg(ap, int);
	int retval = open(path, flags, mode);
	va_end(ap);

	if (retval < 0)
		ut_fatal(file, line, func, "!open: %s", path);

	return retval;
}

/*
 * ut_close -- a close that cannot return -1
 */
int
ut_close(const char *file, int line, const char *func, int fd)
{
	int retval = close(fd);

	if (retval != 0)
		ut_fatal(file, line, func, "!close: %d", fd);

	return retval;
}

/*
 * ut_unlink -- an unlink that cannot return -1
 */
int
ut_unlink(const char *file, int line, const char *func, const char *path)
{
	int retval = unlink(path);

	if (retval != 0)
		ut_fatal(file, line, func, "!unlink: %s", path);

	return retval;
}

/*
 * ut_posix_fallocate -- a posix_fallocate that cannot return -1
 */
int
ut_posix_fallocate(const char *file, int line, const char *func, int fd,
    off_t offset, off_t len)
{
	int retval = posix_fallocate(fd, offset, len);

	if (retval != 0) {
		errno = retval;
		ut_fatal(file, line, func,
		    "!fallocate: fd %d offset 0x%llx len %llu",
		    fd, (unsigned long long)offset, (unsigned long long)len);
	}

	return retval;
}

/*
 * ut_access -- an access that cannot return -1
 */
int
ut_access(const char *file, int line, const char *func, const char *path,
    int mode)
{
	int retval = access(path, mode);

	if (retval != 0)
		ut_fatal(file, line, func, "!access: %s: %d", path, mode);

	return retval;
}

/*
 * ut_write -- a write that can't return -1
 */
size_t
ut_write(const char *file, int line, const char *func, int fd,
    const void *buf, size_t count)
{
	ssize_t retval = write(fd, buf, count);

	if (retval < 0)
		ut_fatal(file, line, func, "!write: %d", fd);

	return (size_t)retval;
}

/*
 * ut_read -- a read that can't return -1
 */
size_t
ut_read(const char *file, int line, const char *func, int fd,
    void *buf, size_t count)
{
	ssize_t retval = read(fd, buf, count);

	if (retval < 0)
		ut_fatal(file, line, func, "!read: %d", fd);

	return (size_t)retval;
}

/*
 * ut_readlink -- a readlink that can't return -1
 */
size_t
ut_readlink(const char *file, int line, const char *func, const char *path,
    void *buf, size_t count)
{
	ssize_t retval = readlink(path, buf, count);

	if (retval < 0)
		ut_fatal(file, line, func, "!readlink: %s", path);

	return (size_t)retval;
}

/*
 * ut_lseek -- an lseek that can't return -1
 */
off_t
ut_lseek(const char *file, int line, const char *func, int fd,
    off_t offset, int whence)
{
	off_t retval = lseek(fd, offset, whence);

	if (retval == -1)
		ut_fatal(file, line, func, "!lseek: %d", fd);

	return retval;
}

int
ut_fcntl(const char *file, int line, const char *func, int fd,
	int cmd, int num, ...)
{

	int retval;
	va_list args;
	uint64_t arg = 0;

	/*
	 * In the case of fcntl, num is always 0 or 1
	 */
	if (num != 0) {
		va_start(args, num);
		arg = va_arg(args, uint64_t);
		retval = fcntl(fd, cmd, arg);
		va_end(args);
	} else
		retval = fcntl(fd, cmd);

	if (retval < 0)
		ut_fatal(file, line, func, "!fcntl: %d", fd);

	return retval;
}

/*
 * ut_fstat -- a fstat that cannot return -1
 */
int
ut_fstat(const char *file, int line, const char *func, int fd,
    struct stat *st_bufp)
{
	int retval = fstat(fd, st_bufp);

	if (retval < 0)
		ut_fatal(file, line, func, "!fstat: %d", fd);

	return retval;
}

/*
 * ut_flock -- a flock that cannot return -1
 */
int
ut_flock(const char *file, int line, const char *func, int fd, int op)
{
	int retval = flock(fd, op);

	if (retval != 0)
		ut_fatal(file, line, func, "!flock: %d", fd);

	return retval;
}

/*
 * ut_stat -- a stat that cannot return -1
 */
int
ut_stat(const char *file, int line, const char *func, const char *path,
    struct stat *st_bufp)
{
	int retval = stat(path, st_bufp);

	if (retval < 0)
		ut_fatal(file, line, func, "!stat: %s", path);

	return retval;
}

/*
 * ut_mmap -- a mmap call that cannot return MAP_FAILED
 */
void *
ut_mmap(const char *file, int line, const char *func, void *addr,
    size_t length, int prot, int flags, int fd, off_t offset)
{
	void *ret_addr = mmap(addr, length, prot, flags, fd, offset);

	if (ret_addr == MAP_FAILED)
		ut_fatal(file, line, func,
		    "!mmap: addr=0x%llx length=0x%zx prot=%d flags=%d fd=%d "
		    "offset=0x%llx", (unsigned long long)addr,
		    length, prot, flags, fd, (unsigned long long)offset);

	return ret_addr;
}

/*
 * ut_munmap -- a munmap call that cannot return -1
 */
int
ut_munmap(const char *file, int line, const char *func, void *addr,
    size_t length)
{
	int retval = munmap(addr, length);

	if (retval < 0)
		ut_fatal(file, line, func, "!munmap: addr=0x%llx length=0x%zx",
		    (unsigned long long)addr, length);

	return retval;
}

/*
 * ut_mprotect -- a mprotect call that cannot return -1
 */
int
ut_mprotect(const char *file, int line, const char *func, void *addr,
    size_t len, int prot)
{
	int retval = mprotect(addr, len, prot);

	if (retval < 0)
		ut_fatal(file, line, func,
		    "!mprotect: addr=0x%llx length=0x%zx prot=0x%x",
		    (unsigned long long)addr, len, prot);

	return retval;
}

/*
 * ut_symlink -- a symlink that cannot return -1
 */
int
ut_symlink(const char *file, int line, const char *func, const char *oldpath,
    const char *newpath)
{
	int retval = symlink(oldpath, newpath);

	if (retval < 0)
		ut_fatal(file, line, func, "!symlink: %s %s", oldpath, newpath);

	return retval;
}

/*
 * ut_link -- a link that cannot return -1
 */
int
ut_link(const char *file, int line, const char *func, const char *oldpath,
    const char *newpath)
{
	int retval = link(oldpath, newpath);

	if (retval < 0)
		ut_fatal(file, line, func, "!link: %s %s", oldpath,
		    newpath);

	return retval;
}

/*
 * ut_mkdir -- a mkdir that cannot return -1
 */
int
ut_mkdir(const char *file, int line, const char *func,
    const char *pathname, mode_t mode)
{
	int retval = mkdir(pathname, mode);

	if (retval < 0)
		ut_fatal(file, line, func, "!mkdir: %s", pathname);

	return retval;
}

/*
 * ut_rmdir -- a rmdir that cannot return -1
 */
int
ut_rmdir(const char *file, int line, const char *func,
    const char *pathname)
{
	int retval = rmdir(pathname);

	if (retval < 0)
		ut_fatal(file, line, func, "!rmdir: %s", pathname);

	return retval;
}

/*
 * ut_rename -- a rename that cannot return -1
 */
int
ut_rename(const char *file, int line, const char *func,
    const char *oldpath, const char *newpath)
{
	int retval = rename(oldpath, newpath);

	if (retval < 0)
		ut_fatal(file, line, func, "!rename: %s %s", oldpath, newpath);

	return retval;
}

/*
 * ut_mount -- a mount that cannot return -1
 */
int
ut_mount(const char *file, int line, const char *func, const char *src,
    const char *tar, const char *fstype, unsigned long flags,
    const void *data)
{
	int retval = mount(src, tar, fstype, flags, data);

	if (retval < 0)
		ut_fatal(file, line, func, "!mount: %s %s %s %lx",
		    src, tar, fstype, flags);

	return retval;
}

/*
 * ut_umount -- a umount that cannot return -1
 */
int
ut_umount(const char *file, int line, const char *func, const char *tar)
{
	int retval = umount(tar);

	if (retval < 0)
		ut_fatal(file, line, func, "!umount: %s", tar);

	return retval;
}


/*
 * ut_truncate -- a truncate that cannot return -1
 */
int
ut_truncate(const char *file, int line, const char *func, const char *path,
    off_t length)
{
	int retval = truncate(path, length);

	if (retval < 0)
		ut_fatal(file, line, func, "!truncate: %s %llu",
				path, (unsigned long long)length);

	return retval;
}

/*
 * ut_ftruncate -- a ftruncate that cannot return -1
 */
int
ut_ftruncate(const char *file, int line, const char *func, int fd,
    off_t length)
{
	int retval = ftruncate(fd, length);

	if (retval < 0)
		ut_fatal(file, line, func, "!ftruncate: %d %llu",
				fd, (unsigned long long)length);

	return retval;
}

/*
 * ut_chmod -- a chmod that cannot return -1
 */
int
ut_chmod(const char *file, int line, const char *func, const char *path,
    mode_t mode)
{
	int retval = chmod(path, mode);

	if (retval < 0)
		ut_fatal(file, line, func, "!mode: %s %o", path, mode);

	return retval;
}

/*
 * ut_mknod -- a mknod that cannot return -1
 */
int
ut_mknod(const char *file, int line, const char *func, const char *pathname,
    mode_t mode, dev_t dev)
{
	int retval = mknod(pathname, mode, dev);

	if (retval < 0)
		ut_fatal(file, line, func, "!mknod: %s", pathname);

	return retval;
}

/*
 * ut_pselect -- a pselect that cannot return -1
 */
int
ut_pselect(const char *file, int line, const char *func, int nfds,
    fd_set *rfds, fd_set *wfds, fd_set *efds, const struct timespec *tv,
    const sigset_t *sigmask)
{
	int retval = pselect(nfds, rfds, wfds, efds, tv, sigmask);

	if (retval < 0)
		ut_fatal(file, line, func, "!pselect");

	return retval;
}

/*
 * ut_opendir -- an opendir that cannot return NULL
 */
DIR *
ut_opendir(const char *file, int line, const char *func,
    const char *name)
{
	DIR *retval = opendir(name);

	if (retval == NULL)
		ut_fatal(file, line, func, "!opendir: %s", name);

	return retval;
}

/*
 * ut_dirfd -- a dirfd that cannot return -1
 */
int
ut_dirfd(const char *file, int line, const char *func,
    DIR *dirp)
{
	int retval = dirfd(dirp);

	if (retval < 0)
		ut_fatal(file, line, func, "!dirfd: %p", dirp);

	return retval;
}

/*
 * ut_closedir -- a closedir that cannot return -1
 */
int
ut_closedir(const char *file, int line, const char *func, DIR *dirp)
{
	int retval = closedir(dirp);

	if (retval < 0)
		ut_fatal(file, line, func, "!closedir: %p", dirp);

	return retval;
}
