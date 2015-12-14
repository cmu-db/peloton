/*
 * Copyright (c) 2015, Intel Corporation
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
 * obj_pmemlog_simple.c -- alternate pmemlog implementation based on pmemobj
 *
 * usage: obj_pmemlog_simple [co] file [cmd[:param]...]
 *
 *   c - create file
 *   o - open file
 *
 * The "cmd" arguments match the pmemlog functions:
 *   a - append
 *   v - appendv
 *   r - rewind
 *   w - walk
 *   n - nbyte
 *   t - tell
 * "a", "w" and "v" require a parameter string(s) separated by a colon
 */

#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/param.h>

#include "libpmemobj.h"
#include "libpmem.h"
#include "libpmemlog.h"

#define	USABLE_SIZE (9.0 / 10)
#define	MAX_POOL_SIZE ((size_t)(1024L * 1024 * 1024 * 16))
#define	POOL_SIZE ((size_t)(1024 * 1024 * 100))

POBJ_LAYOUT_BEGIN(obj_pmemlog_simple);
POBJ_LAYOUT_ROOT(obj_pmemlog_simple, struct base);
POBJ_LAYOUT_TOID(obj_pmemlog_simple, struct log);
POBJ_LAYOUT_END(obj_pmemlog_simple);

/* log entry header */
struct log_hdr {
	off_t write_offset;	/* data write offset */
	size_t data_size;	/* size available for data */
};

/* struct log stores the entire log entry */
struct log {
	struct log_hdr hdr;
	char data[];
};

/* struct base has the lock and log OID */
struct base {
	PMEMrwlock rwlock;	/* lock covering entire log */
	TOID(struct log) log;
};

/*
 * pmemblk_map -- (internal) read or initialize the log pool
 */
static int
pmemlog_map(PMEMobjpool *pop, size_t fsize)
{
	int retval = 0;
	TOID(struct base)bp;
	bp = POBJ_ROOT(pop, struct base);
	/* log already initialized */
	if (!TOID_IS_NULL(D_RO(bp)->log))
		return retval;

	size_t pool_size = fsize * USABLE_SIZE;
	/* max size of a single allocation is 16GB */
	if (pool_size > MAX_POOL_SIZE) {
		errno = EINVAL;
		return 1;
	}

	TX_BEGIN(pop) {
		TX_ADD(bp);
		D_RW(bp)->log = TX_ZALLOC(struct log, pool_size);
		D_RW(D_RW(bp)->log)->hdr.data_size =
				pool_size - sizeof (struct log_hdr);
	} TX_ONABORT {
		retval = -1;
	} TX_END

	return retval;
}

/*
 * pmemlog_open -- pool open wrapper
 */
PMEMlogpool *
pmemlog_open(const char *path)
{
	PMEMobjpool *pop = pmemobj_open(path,
				POBJ_LAYOUT_NAME(obj_pmemlog_simple));
	assert(pop != NULL);
	struct stat buf;
	if (stat(path, &buf)) {
		perror("stat");
		return NULL;
	}

	return pmemlog_map(pop, buf.st_size) ? NULL : (PMEMlogpool *)pop;
}

/*
 * pmemlog_create -- pool create wrapper
 */
PMEMlogpool *
pmemlog_create(const char *path, size_t poolsize, mode_t mode)
{

	PMEMobjpool *pop = pmemobj_create(path,
				POBJ_LAYOUT_NAME(obj_pmemlog_simple),
				poolsize, mode);
	assert(pop != NULL);
	struct stat buf;
	if (stat(path, &buf)) {
		perror("stat");
		return NULL;
	}

	return pmemlog_map(pop, buf.st_size) ? NULL : (PMEMlogpool *)pop;
}

/*
 * pool_close -- pool close wrapper
 */
void
pmemlog_close(PMEMlogpool *plp)
{
	pmemobj_close((PMEMobjpool *)plp);
}

/*
 * pmemlog_nbyte -- return usable size of a log memory pool
 */
size_t
pmemlog_nbyte(PMEMlogpool *plp)
{
	PMEMobjpool *pop = (PMEMobjpool *)plp;

	TOID(struct log) logp;
	logp = D_RO(POBJ_ROOT(pop, struct base))->log;
	return D_RO(logp)->hdr.data_size;
}

/*
 * pmemlog_append -- add data to a log memory pool
 */
int
pmemlog_append(PMEMlogpool *plp, const void *buf, size_t count)
{
	PMEMobjpool *pop = (PMEMobjpool *)plp;
	int retval = 0;
	TOID(struct base) bp;
	bp = POBJ_ROOT(pop, struct base);

	TOID(struct log) logp;
	logp = D_RW(bp)->log;
	/* check for overrun */
	if ((D_RO(logp)->hdr.write_offset + count)
			> D_RO(logp)->hdr.data_size) {
		errno = ENOMEM;
		return 1;
	}

	/* begin a transaction, also acquiring the write lock for the log */
	TX_BEGIN_LOCK(pop, TX_LOCK_RWLOCK, &D_RW(bp)->rwlock, TX_LOCK_NONE) {
		char *dst = D_RW(logp)->data + D_RO(logp)->hdr.write_offset;
		/* add hdr to undo log */
		TX_ADD_FIELD(logp, hdr);
		/* copy and persist data */
		pmemobj_memcpy_persist(pop, dst, buf, count);
		/* set the new offset */
		D_RW(logp)->hdr.write_offset += count;
	} TX_ONABORT {
		retval = -1;
	} TX_END

	return retval;
}

/*
 * pmemlog_appendv -- add gathered data to a log memory pool
 */
int
pmemlog_appendv(PMEMlogpool *plp, const struct iovec *iov, int iovcnt)
{
	PMEMobjpool *pop = (PMEMobjpool *)plp;
	int retval = 0;
	TOID(struct base) bp;
	bp = POBJ_ROOT(pop, struct base);

	uint64_t total_count = 0;
	/* calculate required space */
	for (int i = 0; i < iovcnt; ++i)
		total_count += iov[i].iov_len;

	TOID(struct log) logp;
	logp = D_RW(bp)->log;
	/* check for overrun */
	if ((D_RO(logp)->hdr.write_offset + total_count)
			> D_RO(logp)->hdr.data_size) {
		errno = ENOMEM;
		return 1;
	}

	/* begin a transaction, also acquiring the write lock for the log */
	TX_BEGIN_LOCK(pop, TX_LOCK_RWLOCK, &D_RW(bp)->rwlock, TX_LOCK_NONE) {
		TX_ADD(D_RW(bp)->log);
		/* append the data */
		for (int i = 0; i < iovcnt; ++i) {
			char *buf = iov[i].iov_base;
			size_t count = iov[i].iov_len;
			char *dst = D_RW(logp)->data
				+ D_RO(logp)->hdr.write_offset;
			/* copy and persist data */
			pmemobj_memcpy_persist(pop, dst, buf, count);
			/* set the new offset */
			D_RW(logp)->hdr.write_offset += count;
		}
	} TX_ONABORT {
		retval = -1;
	} TX_END

	return retval;
}

/*
 * pmemlog_tell -- return current write point in a log memory pool
 */
off_t
pmemlog_tell(PMEMlogpool *plp)
{
	PMEMobjpool *pop = (PMEMobjpool *)plp;

	TOID(struct log) logp;
	logp = D_RO(POBJ_ROOT(pop, struct base))->log;
	return D_RO(logp)->hdr.write_offset;
}

/*
 * pmemlog_rewind -- discard all data, resetting a log memory pool to empty
 */
void
pmemlog_rewind(PMEMlogpool *plp)
{
	PMEMobjpool *pop = (PMEMobjpool *)plp;
	TOID(struct base) bp;
	bp = POBJ_ROOT(pop, struct base);

	/* begin a transaction, also acquiring the write lock for the log */
	TX_BEGIN_LOCK(pop, TX_LOCK_RWLOCK, &D_RW(bp)->rwlock, TX_LOCK_NONE) {
		/* add the hdr to the undo log */
		TX_ADD_FIELD(D_RW(bp)->log, hdr);
		/* reset the write offset */
		D_RW(D_RW(bp)->log)->hdr.write_offset = 0;
	} TX_END
}

/*
 * pmemlog_walk -- walk through all data in a log memory pool
 *
 * chunksize of 0 means process_chunk gets called once for all data
 * as a single chunk.
 */
void
pmemlog_walk(PMEMlogpool *plp, size_t chunksize,
	int (*process_chunk)(const void *buf, size_t len, void *arg), void *arg)
{
	PMEMobjpool *pop = (PMEMobjpool *)plp;
	TOID(struct base) bp;
	bp = POBJ_ROOT(pop, struct base);

	/* acquire a rdlock here */
	int err;
	if ((err = pmemobj_rwlock_rdlock(pop, &D_RW(bp)->rwlock)) != 0) {
		errno = err;
		return;
	}

	TOID(struct log) logp;
	logp = D_RW(bp)->log;

	size_t read_size = chunksize ? : D_RO(logp)->hdr.data_size;

	char *read_ptr = D_RW(logp)->data;
	const char *write_ptr = (D_RO(logp)->data
					+ D_RO(logp)->hdr.write_offset);
	while (read_ptr	< write_ptr) {
		read_size = MIN(read_size, write_ptr - read_ptr);
		(*process_chunk)(read_ptr, read_size, arg);
		read_ptr += read_size;
	}

	pmemobj_rwlock_unlock(pop, &D_RW(bp)->rwlock);
}

/*
 * process_chunk -- (internal) process function for log_walk
 */
static int
process_chunk(const void *buf, size_t len, void *arg)
{
	char tmp[len + 1];

	memcpy(tmp, buf, len);
	tmp[len] = '\0';

	printf("log contains:\n");
	printf("%s\n", tmp);
	return 1; /* continue */
}

/*
 * count_iovec -- (internal) count the number of iovec items
 */
static int
count_iovec(char *arg)
{
	int count = 1;
	char *pch = strchr(arg, ':');
	while (pch != NULL) {
		++count;
		pch = strchr(++pch, ':');
	}

	return count;
}

/*
 * fill_iovec -- (internal) fill out the iovec
 */
static void
fill_iovec(struct iovec *iov, char *arg)
{
	char *pch = strtok(arg, ":");
	while (pch != NULL) {
		iov->iov_base = pch;
		iov->iov_len = strlen((char *)iov->iov_base);
		++iov;
		pch = strtok(NULL, ":");
	}
}

int
main(int argc, char *argv[])
{

	if (argc < 2) {
		fprintf(stderr, "usage: %s [o,c] file [val...]", argv[0]);
		return 1;
	}

	PMEMlogpool *plp;
	if (strncmp(argv[1], "c", 1) == 0) {
		plp = pmemlog_create(argv[2], POOL_SIZE, S_IRUSR | S_IWUSR);
	} else if (strncmp(argv[1], "o", 1) == 0) {
		plp = pmemlog_open(argv[2]);
	} else {
		fprintf(stderr, "usage: %s [o,c] file [val...]", argv[0]);
		return 1;
	}

	if (plp == NULL) {
		perror("pmemlog_create/pmemlog_open");
		return 1;
	}

	/* process the command line arguments */
	for (int i = 3; i < argc; i++) {
		switch (*argv[i]) {
			case 'a': {
				printf("append: %s\n", argv[i] + 2);
				pmemlog_append(plp, argv[i] + 2,
					strlen(argv[i] + 2));
				break;
			}
			case 'v': {
				printf("appendv: %s\n", argv[i] + 2);
				int count = count_iovec(argv[i] + 2);
				struct iovec *iov = malloc(count
						* sizeof (struct iovec));
				assert(iov != NULL);
				fill_iovec(iov, argv[i] + 2);
				pmemlog_appendv(plp, iov, count);
				free(iov);
				break;
			}
			case 'r': {
				printf("rewind\n");
				pmemlog_rewind(plp);
				break;
			}
			case 'w': {
				printf("walk\n");
				unsigned long walksize = strtoul(argv[i] + 2,
					NULL, 10);
				pmemlog_walk(plp, walksize, process_chunk,
					NULL);
				break;
			}
			case 'n': {
				printf("nbytes: %zu\n", pmemlog_nbyte(plp));
				break;
			}
			case 't': {
				printf("offset: %ld\n", pmemlog_tell(plp));
				break;
			}
			default: {
				fprintf(stderr, "unrecognized command %s\n",
					argv[i]);
				break;
			}
		};
	}

	/* all done */
	pmemlog_close(plp);

	return 0;
}
