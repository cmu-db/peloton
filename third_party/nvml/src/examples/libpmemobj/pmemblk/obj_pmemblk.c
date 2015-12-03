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
 * obj_pmemblk.c -- alternate pmemblk implementation based on pmemobj
 *
 * usage: obj_pmemblk [co] file blk_size [cmd[:blk_num[:data]]...]
 *
 *   c - create file
 *   o - open file
 *
 * The "cmd" arguments match the pmemblk functions:
 *   w - write to a block
 *   r - read a block
 *   z - zero a block
 *   n - write out number of available blocks
 */

#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>

#include "libpmemobj.h"
#include "libpmem.h"
#include "libpmemblk.h"

#define	USABLE_SIZE (9.0 / 10)
#define	POOL_SIZE ((size_t)(1024 * 1024 * 50))
#define	MAX_POOL_SIZE ((size_t)(1024L * 1024 * 1024 * 16))
#define	MAX_THREADS 256
#define	BSIZE_MAX ((size_t)(1024 * 1024 * 10))

POBJ_LAYOUT_BEGIN(obj_pmemblk);
POBJ_LAYOUT_ROOT(obj_pmemblk, struct base);
POBJ_LAYOUT_TOID(obj_pmemblk, uint8_t);
POBJ_LAYOUT_END(obj_pmemblk);

/* The root object struct holding all necessary data */
struct base {
	TOID(uint8_t) data;		/* contiguous memory region */
	size_t bsize;			/* block size */
	size_t nblocks;			/* number of available blocks */
	PMEMmutex locks[MAX_THREADS];	/* thread synchronization locks */
};

/*
 * pmemblk_map -- (internal) read or initialize the blk pool
 */
static int
pmemblk_map(PMEMobjpool *pop, size_t bsize, size_t fsize)
{
	int retval = 0;
	TOID(struct base) bp;
	bp = POBJ_ROOT(pop, struct base);

	/* read pool descriptor and validate user provided values */
	if (D_RO(bp)->bsize) {
		if (bsize && D_RO(bp)->bsize != bsize)
			return -1;
		else
			return 0;
	}

	/* new pool, calculate and store metadata */
	TX_BEGIN(pop) {
		TX_ADD(bp);
		D_RW(bp)->bsize = bsize;
		size_t pool_size = fsize * USABLE_SIZE;
		D_RW(bp)->nblocks = pool_size / bsize;
		D_RW(bp)->data = TX_ZALLOC(uint8_t, pool_size);
	} TX_ONABORT {
		retval = -1;
	} TX_END

	return retval;
}

/*
 * pmemblk_open -- open a block memory pool
 */
PMEMblkpool *
pmemblk_open(const char *path, size_t bsize)
{
	PMEMobjpool *pop = pmemobj_open(path, POBJ_LAYOUT_NAME(obj_pmemblk));
	if (pop == NULL)
		return NULL;
	struct stat buf;
	if (stat(path, &buf)) {
		perror("stat");
		return NULL;
	}

	return pmemblk_map(pop, bsize, buf.st_size) ? NULL : (PMEMblkpool *)pop;

}

/*
 * pmemblk_create -- create a block memory pool
 */
PMEMblkpool *
pmemblk_create(const char *path, size_t bsize, size_t poolsize, mode_t mode)
{
	/* max size of a single allocation is 16GB */
	if (poolsize > MAX_POOL_SIZE) {
		errno = EINVAL;
		return NULL;
	}

	PMEMobjpool *pop = pmemobj_create(path, POBJ_LAYOUT_NAME(obj_pmemblk),
				poolsize, mode);
	if (pop == NULL)
		return NULL;

	return pmemblk_map(pop, bsize, poolsize) ? NULL : (PMEMblkpool *)pop;
}

/*
 * pmemblk_close -- close a block memory pool
 */
void
pmemblk_close(PMEMblkpool *pbp)
{
	pmemobj_close((PMEMobjpool *)pbp);
}

/*
 * pmemblk_check -- block memory pool consistency check
 */
int
pmemblk_check(const char *path, size_t bsize)
{
	int ret = pmemobj_check(path, POBJ_LAYOUT_NAME(obj_pmemblk));
	if (ret)
		return ret;

	/* open just to validate block size */
	PMEMblkpool *pop = pmemblk_open(path, bsize);
	if (!pop)
		return -1;

	pmemblk_close(pop);
	return 0;
}

/*
 * pmemblk_set_error -- not available in this implementation
 */
int
pmemblk_set_error(PMEMblkpool *pbp, off_t blockno)
{
	/* N/A */
	return 0;
}

/*
 * pmemblk_nblock -- return number of usable blocks in a block memory pool
 */
size_t
pmemblk_nblock(PMEMblkpool *pbp)
{
	PMEMobjpool *pop = (PMEMobjpool *)pbp;
	return ((struct base *)pmemobj_direct(pmemobj_root(pop,
					sizeof (struct base))))->nblocks;
}

/*
 * pmemblk_read -- read a block in a block memory pool
 */
int
pmemblk_read(PMEMblkpool *pbp, void *buf, off_t blockno)
{
	PMEMobjpool *pop = (PMEMobjpool *)pbp;
	TOID(struct base) bp;
	bp = POBJ_ROOT(pop, struct base);

	if (blockno >= D_RO(bp)->nblocks)
		return 1;

	pmemobj_mutex_lock(pop, &D_RW(bp)->locks[blockno % MAX_THREADS]);
	size_t block_off = blockno * D_RO(bp)->bsize;
	uint8_t *src = D_RW(D_RW(bp)->data) + block_off;
	memcpy(buf, src, D_RO(bp)->bsize);
	pmemobj_mutex_unlock(pop, &D_RW(bp)->locks[blockno % MAX_THREADS]);

	return 0;
}

/*
 * pmemblk_write -- write a block (atomically) in a block memory pool
 */
int
pmemblk_write(PMEMblkpool *pbp, const void *buf, off_t blockno)
{
	PMEMobjpool *pop = (PMEMobjpool *)pbp;
	int retval = 0;
	TOID(struct base) bp;
	bp = POBJ_ROOT(pop, struct base);

	if (blockno >= D_RO(bp)->nblocks)
		return 1;

	TX_BEGIN_LOCK(pop, TX_LOCK_MUTEX,
		&D_RW(bp)->locks[blockno % MAX_THREADS], TX_LOCK_NONE) {
		size_t block_off = blockno * D_RO(bp)->bsize;
		uint8_t *dst = D_RW(D_RW(bp)->data) + block_off;
		/* add the modified block to the undo log */
		pmemobj_tx_add_range_direct(dst, D_RO(bp)->bsize);
		memcpy(dst, buf, D_RO(bp)->bsize);
	} TX_ONABORT {
		retval = 1;
	} TX_END

	return retval;
}

/*
 * pmemblk_set_zero -- zero a block in a block memory pool
 */
int
pmemblk_set_zero(PMEMblkpool *pbp, off_t blockno)
{
	PMEMobjpool *pop = (PMEMobjpool *)pbp;
	int retval = 0;
	TOID(struct base) bp;
	bp = POBJ_ROOT(pop, struct base);

	if (blockno >= D_RO(bp)->nblocks)
		return 1;

	TX_BEGIN_LOCK(pop, TX_LOCK_MUTEX,
		&D_RW(bp)->locks[blockno % MAX_THREADS], TX_LOCK_NONE) {
		size_t block_off = blockno * D_RO(bp)->bsize;
		uint8_t *dst = D_RW(D_RW(bp)->data) + block_off;
		pmemobj_tx_add_range_direct(dst, D_RO(bp)->bsize);
		memset(dst, 0, D_RO(bp)->bsize);
	} TX_ONABORT {
		retval = -1;
	} TX_END

	return retval;
}

int
main(int argc, char *argv[])
{
	if (argc < 4) {
		fprintf(stderr, "usage: %s [co] file blk_size"\
			" [cmd[:blk_num[:data]]...]", argv[0]);
		return 1;
	}

	unsigned long bsize = strtoul(argv[3], NULL, 10);
	assert(bsize <= BSIZE_MAX);

	PMEMblkpool *pbp;
	if (strncmp(argv[1], "c", 1) == 0) {
		pbp = pmemblk_create(argv[2], bsize, POOL_SIZE,
			S_IRUSR | S_IWUSR);
	} else if (strncmp(argv[1], "o", 1) == 0) {
		pbp = pmemblk_open(argv[2], bsize);
	} else {
		fprintf(stderr, "usage: %s [co] file blk_size"
			" [cmd[:blk_num[:data]]...]", argv[0]);
		return 1;
	}

	if (pbp == NULL) {
		perror("pmemblk_create/pmemblk_open");
		return 1;
	}

	/* process the command line arguments */
	for (int i = 4; i < argc; i++) {
		switch (*argv[i]) {
			case 'w': {
				printf("write: %s\n", argv[i] + 2);
				const char *block_str = strtok(argv[i] + 2,
							":");
				const char *data = strtok(NULL, ":");
				assert(block_str != NULL);
				assert(data != NULL);
				unsigned long block = strtoul(block_str,
							NULL, 10);
				pmemblk_write(pbp, data, block);
				break;
			}
			case 'r': {
				printf("read: %s\n", argv[i] + 2);
				char *buf = malloc(bsize + 1);
				assert(buf != NULL);
				const char *block_str = strtok(argv[i] + 2,
							":");
				assert(block_str != NULL);
				pmemblk_read(pbp, buf, strtoul(block_str, NULL,
							10));
				buf[bsize] = '\0';
				printf("%s\n", buf);
				free(buf);
				break;
			}
			case 'z': {
				printf("zero: %s\n", argv[i] + 2);
				const char *block_str = strtok(argv[i] + 2,
							":");
				assert(block_str != NULL);
				pmemblk_set_zero(pbp, strtoul(block_str, NULL,
							10));
				break;
			}
			case 'n': {
				printf("nblocks: ");
				printf("%zu\n", pmemblk_nblock(pbp));
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
	pmemblk_close(pbp);

	return 0;
}
