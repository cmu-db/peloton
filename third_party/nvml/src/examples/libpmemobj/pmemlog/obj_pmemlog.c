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
 * obj_pmemlog.c -- alternate pmemlog implementation based on pmemobj
 *
 * usage: obj_pmemlog [co] file [cmd[:param]...]
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
 * "a" and "v" require a parameter string(s) separated by a colon
 */

#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>

#include "libpmemobj.h"
#include "libpmem.h"
#include "libpmemlog.h"

#define	LAYOUT_NAME "obj_pmemlog"

#define	POOL_SIZE ((size_t)(1024 * 1024 * 100))

/* types of allocations */
enum types {
	LOG_TYPE,
	LOG_HDR_TYPE,
	BASE_TYPE,

	MAX_TYPES
};


/* log entry header */
struct log_hdr {
	PMEMoid next;		/* object ID of the next log buffer */
	size_t size;		/* size of this log buffer */
};

/* struct log stores the entire log entry */
struct log {
	struct log_hdr hdr;	/* entry header */
	char data[];		/* log entry data */
};

/* struct base keeps track of the beginning of the log list */
struct base {
	PMEMoid head;		/* object ID of the first log buffer */
	PMEMoid tail;		/* object ID of the last log buffer */
	PMEMrwlock rwlock;	/* lock covering entire log */
	size_t bytes_written;	/* number of bytes stored in the pool */
};

/*
 * pmemlog_open -- pool open wrapper
 */
PMEMlogpool *
pmemlog_open(const char *path)
{
	return (PMEMlogpool *)pmemobj_open(path, LAYOUT_NAME);
}

/*
 * pmemlog_create -- pool create wrapper
 */
PMEMlogpool *
pmemlog_create(const char *path, size_t poolsize, mode_t mode)
{
	return (PMEMlogpool *)pmemobj_create(path, LAYOUT_NAME,
				poolsize, mode);
}

/*
 * pmemlog_close -- pool close wrapper
 */
void
pmemlog_close(PMEMlogpool *plp)
{
	pmemobj_close((PMEMobjpool *)plp);
}

/*
 * pmemlog_nbyte -- not available in this implementation
 */
size_t
pmemlog_nbyte(PMEMlogpool *plp)
{
	/* N/A */
	return 0;
}

/*
 * pmemlog_append -- add data to a log memory pool
 */
int
pmemlog_append(PMEMlogpool *plp, const void *buf, size_t count)
{
	PMEMobjpool *pop = (PMEMobjpool *)plp;
	PMEMoid baseoid = pmemobj_root(pop, sizeof (struct base));
	struct base *bp = pmemobj_direct(baseoid);

	/* set the return point */
	jmp_buf env;
	if (setjmp(env)) {
		/* end the transaction */
		pmemobj_tx_end();
		return 1;
	}

	/* begin a transaction, also acquiring the write lock for the log */
	pmemobj_tx_begin(pop, env, TX_LOCK_RWLOCK, &bp->rwlock, TX_LOCK_NONE);

	/* allocate the new node to be inserted */
	PMEMoid log = pmemobj_tx_alloc(count + sizeof (struct log_hdr),
				LOG_TYPE);

	struct log *logp = pmemobj_direct(log);
	logp->hdr.size = count;
	memcpy(logp->data, buf, count);
	logp->hdr.next = OID_NULL;

	/* add the modified root object to the undo log */
	pmemobj_tx_add_range(baseoid, 0, sizeof (struct base));
	if (bp->tail.off == 0) {
		/* update head */
		bp->head = log;
	} else {
		/* add the modified tail entry to the undo log */
		pmemobj_tx_add_range(bp->tail, 0, sizeof (struct log));
		((struct log *)pmemobj_direct(bp->tail))->hdr.next = log;
	}

	bp->tail = log; /* update tail */
	bp->bytes_written += count;

	pmemobj_tx_commit();
	pmemobj_tx_end();
	return 0;
}

/*
 * pmemlog_appendv -- add gathered data to a log memory pool
 */
int
pmemlog_appendv(PMEMlogpool *plp, const struct iovec *iov, int iovcnt)
{
	PMEMobjpool *pop = (PMEMobjpool *)plp;
	PMEMoid baseoid = pmemobj_root(pop, sizeof (struct base));
	struct base *bp = pmemobj_direct(baseoid);

	/* set the return point */
	jmp_buf env;
	if (setjmp(env)) {
		/* end the transaction */
		pmemobj_tx_end();
		return 1;
	}

	/* begin a transaction, also acquiring the write lock for the log */
	pmemobj_tx_begin(pop, env, TX_LOCK_RWLOCK, &bp->rwlock, TX_LOCK_NONE);

	/* add the base object to the undo log - once for the transaction */
	pmemobj_tx_add_range(baseoid, 0, sizeof (struct base));
	/* add the tail entry once to the undo log */
	pmemobj_tx_add_range(bp->tail, 0, sizeof (struct log));

	/* append the data */
	for (int i = 0; i < iovcnt; ++i) {
		char *buf = iov[i].iov_base;
		size_t count = iov[i].iov_len;

		/* allocate the new node to be inserted */
		PMEMoid log = pmemobj_tx_alloc(count + sizeof (struct log_hdr),
				LOG_TYPE);

		struct log *logp = pmemobj_direct(log);
		logp->hdr.size = count;
		memcpy(logp->data, buf, count);
		logp->hdr.next = OID_NULL;

		if (bp->tail.off == 0) {
			bp->head = log;	/* update head */
		} else {
			((struct log *)pmemobj_direct(bp->tail))->hdr.next =
							log;
		}

		bp->tail = log; /* update tail */
		bp->bytes_written += count;
	}

	pmemobj_tx_commit();
	pmemobj_tx_end();
	return 0;
}

/*
 * pmemlog_tell -- returns the current write point for the log
 */
off_t
pmemlog_tell(PMEMlogpool *plp)
{
	PMEMobjpool *pop = (PMEMobjpool *)plp;
	struct base *bp = pmemobj_direct(pmemobj_root(pop,
				sizeof (struct base)));

	if (pmemobj_rwlock_rdlock(pop, &bp->rwlock) != 0)
		return 0;

	off_t bytes_written = bp->bytes_written;

	pmemobj_rwlock_unlock(pop, &bp->rwlock);

	return bytes_written;
}

/*
 * pmemlog_rewind -- discard all data, resetting a log memory pool to empty
 */
void
pmemlog_rewind(PMEMlogpool *plp)
{
	PMEMobjpool *pop = (PMEMobjpool *)plp;
	PMEMoid baseoid = pmemobj_root(pop, sizeof (struct base));
	struct base *bp = pmemobj_direct(baseoid);

	/* set the return point */
	jmp_buf env;
	if (setjmp(env)) {
		/* end the transaction */
		pmemobj_tx_end();
		return;
	}

	/* begin a transaction, also acquiring the write lock for the log */
	pmemobj_tx_begin(pop, env, TX_LOCK_RWLOCK, &bp->rwlock, TX_LOCK_NONE);
	/* add the root object to the undo log */
	pmemobj_tx_add_range(baseoid, 0, sizeof (struct base));

	/* free all log nodes */
	while (bp->head.off != 0) {
		PMEMoid nextoid =
			((struct log *)pmemobj_direct(bp->head))->hdr.next;
		pmemobj_tx_free(bp->head);
		bp->head = nextoid;
	}

	bp->head = OID_NULL;
	bp->tail = OID_NULL;
	bp->bytes_written = 0;

	pmemobj_tx_commit();
	pmemobj_tx_end();
}

/*
 * pmemlog_walk -- walk through all data in a log memory pool
 *
 * As this implementation holds the size of each entry, the chunksize is ignored
 * and the process_chunk function gets the actual entry length.
 */
void
pmemlog_walk(PMEMlogpool *plp, size_t chunksize,
	int (*process_chunk)(const void *buf, size_t len, void *arg), void *arg)
{
	PMEMobjpool *pop = (PMEMobjpool *)plp;
	struct base *bp = pmemobj_direct(pmemobj_root(pop,
						sizeof (struct base)));

	if (pmemobj_rwlock_rdlock(pop, &bp->rwlock) != 0)
		return;

	/* process all chunks */
	struct log *next = pmemobj_direct(bp->head);
	while (next != NULL) {
		(*process_chunk)(next->data, next->hdr.size, arg);
		next = pmemobj_direct(next->hdr.next);
	}

	pmemobj_rwlock_unlock(pop, &bp->rwlock);
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
	return 1;
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
				pmemlog_walk(plp, 0, process_chunk, NULL);
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
