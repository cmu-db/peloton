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
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY LOG OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * blk.c -- block memory pool entry points for libpmem
 */

#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/param.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <stdint.h>
#include <uuid/uuid.h>
#include <pthread.h>

#include "libpmem.h"
#include "libpmemblk.h"

#include "util.h"
#include "out.h"
#include "btt.h"
#include "blk.h"
#include "valgrind_internal.h"

/*
 * lane_enter -- (internal) acquire a unique lane number
 */
static int
lane_enter(PMEMblkpool *pbp, unsigned *lane)
{
	unsigned mylane;

	mylane = __sync_fetch_and_add(&pbp->next_lane, 1) % pbp->nlane;

	/* lane selected, grab the per-lane lock */
	if ((errno = pthread_mutex_lock(&pbp->locks[mylane]))) {
		ERR("!pthread_mutex_lock");
		return -1;
	}

	*lane = mylane;

	return 0;
}

/*
 * lane_exit -- (internal) drop lane lock
 */
static void
lane_exit(PMEMblkpool *pbp, unsigned mylane)
{
	int oerrno = errno;
	if ((errno = pthread_mutex_unlock(&pbp->locks[mylane])))
		ERR("!pthread_mutex_unlock");
	errno = oerrno;
}

/*
 * nsread -- (internal) read data from the namespace encapsulating the BTT
 *
 * This routine is provided to btt_init() to allow the btt module to
 * do I/O on the memory pool containing the BTT layout.
 */
static int
nsread(void *ns, unsigned lane, void *buf, size_t count, uint64_t off)
{
	struct pmemblk *pbp = (struct pmemblk *)ns;

	LOG(13, "pbp %p lane %u count %zu off %ju", pbp, lane, count, off);

	if (off + count > pbp->datasize) {
		ERR("offset + count (%zu) past end of data area (%zu)",
				off + count, pbp->datasize);
		errno = EINVAL;
		return -1;
	}

	memcpy(buf, (char *)pbp->data + off, count);

	return 0;
}

/*
 * nswrite -- (internal) write data to the namespace encapsulating the BTT
 *
 * This routine is provided to btt_init() to allow the btt module to
 * do I/O on the memory pool containing the BTT layout.
 */
static int
nswrite(void *ns, unsigned lane, const void *buf, size_t count,
		uint64_t off)
{
	struct pmemblk *pbp = (struct pmemblk *)ns;

	LOG(13, "pbp %p lane %u count %zu off %ju", pbp, lane, count, off);

	if (off + count > pbp->datasize) {
		ERR("offset + count (%zu) past end of data area (%zu)",
				off + count, pbp->datasize);
		errno = EINVAL;
		return -1;
	}

	void *dest = (char *)pbp->data + off;

#ifdef DEBUG
	/* grab debug write lock */
	if ((errno = pthread_mutex_lock(&pbp->write_lock))) {
		ERR("!pthread_mutex_lock");
		return -1;
	}
#endif

	/* unprotect the memory (debug version only) */
	RANGE_RW(dest, count);

	if (pbp->is_pmem)
		pmem_memcpy_nodrain(dest, buf, count);
	else
		memcpy(dest, buf, count);

	/* protect the memory again (debug version only) */
	RANGE_RO(dest, count);

#ifdef DEBUG
	/* release debug write lock */
	if ((errno = pthread_mutex_unlock(&pbp->write_lock)))
		ERR("!pthread_mutex_unlock");
#endif

	if (pbp->is_pmem)
		pmem_drain();
	else
		pmem_msync(dest, count);

	return 0;
}

/*
 * nsmap -- (internal) allow direct access to a range of a namespace
 *
 * The caller requests a range to be "mapped" but the return value
 * may indicate a smaller amount (in which case the caller is expected
 * to call back later for another mapping).
 *
 * This routine is provided to btt_init() to allow the btt module to
 * do I/O on the memory pool containing the BTT layout.
 */
static ssize_t
nsmap(void *ns, unsigned lane, void **addrp, size_t len, uint64_t off)
{
	struct pmemblk *pbp = (struct pmemblk *)ns;

	LOG(12, "pbp %p lane %u len %zu off %ju", pbp, lane, len, off);

	ASSERT(((ssize_t)len) >= 0);

	if (off + len >= pbp->datasize) {
		ERR("offset + len (%zu) past end of data area (%zu)",
				off + len, pbp->datasize - 1);
		errno = EINVAL;
		return -1;
	}

	/*
	 * Since the entire file is memory-mapped, this callback
	 * can always provide the entire length requested.
	 */
	*addrp = (char *)pbp->data + off;

	LOG(12, "returning addr %p", *addrp);

	return (ssize_t)len;
}

/*
 * nssync -- (internal) flush changes made to a namespace range
 *
 * This is used in conjunction with the addresses handed out by
 * nsmap() above.  There's no need to sync things written via
 * nswrite() since those changes are flushed each time nswrite()
 * is called.
 *
 * This routine is provided to btt_init() to allow the btt module to
 * do I/O on the memory pool containing the BTT layout.
 */
static void
nssync(void *ns, unsigned lane, void *addr, size_t len)
{
	struct pmemblk *pbp = (struct pmemblk *)ns;

	LOG(12, "pbp %p lane %u addr %p len %zu", pbp, lane, addr, len);

	if (pbp->is_pmem)
		pmem_persist(addr, len);
	else
		pmem_msync(addr, len);
}

/*
 * nszero -- (internal) zero data in the namespace encapsulating the BTT
 *
 * This routine is provided to btt_init() to allow the btt module to
 * zero the memory pool containing the BTT layout.
 */
static int
nszero(void *ns, unsigned lane, size_t count, uint64_t off)
{
	struct pmemblk *pbp = (struct pmemblk *)ns;

	LOG(13, "pbp %p lane %u count %zu off %ju", pbp, lane, count, off);

	if (off + count > pbp->datasize) {
		ERR("offset + count (%zu) past end of data area (%zu)",
				off + count, pbp->datasize);
		errno = EINVAL;
		return -1;
	}

	void *dest = (char *)pbp->data + off;

	/* unprotect the memory (debug version only) */
	RANGE_RW(dest, count);

	pmem_memset_persist(dest, 0, count);

	/* protect the memory again (debug version only) */
	RANGE_RO(dest, count);

	return 0;
}

/* callbacks for btt_init() */
static struct ns_callback ns_cb = {
	.nsread = nsread,
	.nswrite = nswrite,
	.nszero = nszero,
	.nsmap = nsmap,
	.nssync = nssync,
	.ns_is_zeroed = 0
};

/*
 * pmemblk_descr_create -- (internal) create block memory pool descriptor
 */
static int
pmemblk_descr_create(PMEMblkpool *pbp, uint32_t bsize, int zeroed)
{
	LOG(3, "pbp %p bsize %u zeroed %d", pbp, bsize, zeroed);

	/* create the required metadata */
	pbp->bsize = htole32(bsize);
	pmem_msync(&pbp->bsize, sizeof (bsize));

	pbp->is_zeroed = zeroed;
	pmem_msync(&pbp->is_zeroed, sizeof (pbp->is_zeroed));

	return 0;
}

/*
 * pmemblk_descr_check -- (internal) validate block memory pool descriptor
 */
static int
pmemblk_descr_check(PMEMblkpool *pbp, size_t *bsize)
{
	LOG(3, "pbp %p bsize %zu", pbp, *bsize);

	size_t hdr_bsize = le32toh(pbp->bsize);
	if (*bsize && *bsize != hdr_bsize) {
		ERR("wrong bsize (%zu), pool created with bsize %zu",
				*bsize, hdr_bsize);
		errno = EINVAL;
		return -1;
	}
	*bsize = hdr_bsize;
	LOG(3, "using block size from header: %zu", *bsize);

	return 0;
}

/*
 * pmemblk_runtime_init -- (internal) initialize block memory pool runtime data
 */
static int
pmemblk_runtime_init(PMEMblkpool *pbp, size_t bsize, int rdonly, int is_pmem)
{
	LOG(3, "pbp %p bsize %zu rdonly %d is_pmem %d",
			pbp, bsize, rdonly, is_pmem);

	/* remove volatile part of header */
	VALGRIND_REMOVE_PMEM_MAPPING(&pbp->addr,
			sizeof (struct pmemblk) -
			sizeof (struct pool_hdr) -
			sizeof (pbp->bsize) -
			sizeof (pbp->is_zeroed));

	/*
	 * Use some of the memory pool area for run-time info.  This
	 * run-time state is never loaded from the file, it is always
	 * created here, so no need to worry about byte-order.
	 */
	pbp->rdonly = rdonly;
	pbp->is_pmem = is_pmem;
	pbp->data = (char *)pbp->addr +
			roundup(sizeof (*pbp), BLK_FORMAT_DATA_ALIGN);
	ASSERT(((char *)pbp->addr + pbp->size) >= (char *)pbp->data);
	pbp->datasize = (size_t)
			(((char *)pbp->addr + pbp->size) - (char *)pbp->data);

	LOG(4, "data area %p data size %zu bsize %zu",
		pbp->data, pbp->datasize, bsize);

	long ncpus = sysconf(_SC_NPROCESSORS_ONLN);
	if (ncpus < 1)
		ncpus = 1;

	ns_cb.ns_is_zeroed = pbp->is_zeroed;

	/* things free by "goto err" if not NULL */
	struct btt *bttp = NULL;
	pthread_mutex_t *locks = NULL;

	bttp = btt_init(pbp->datasize, (uint32_t)bsize, pbp->hdr.poolset_uuid,
			(unsigned)ncpus * 2, pbp, &ns_cb);

	if (bttp == NULL)
		goto err;	/* btt_init set errno, called LOG */

	pbp->bttp = bttp;

	pbp->nlane = btt_nlane(pbp->bttp);
	pbp->next_lane = 0;
	if ((locks = Malloc(pbp->nlane * sizeof (*locks))) == NULL) {
		ERR("!Malloc for lane locks");
		goto err;
	}

	for (unsigned i = 0; i < pbp->nlane; i++)
		if ((errno = pthread_mutex_init(&locks[i], NULL))) {
			ERR("!pthread_mutex_init");
			goto err;
		}

	pbp->locks = locks;

#ifdef DEBUG
	/* initialize debug lock */
	if ((errno = pthread_mutex_init(&pbp->write_lock, NULL))) {
		ERR("!pthread_mutex_init");
		goto err;
	}
#endif

	/*
	 * If possible, turn off all permissions on the pool header page.
	 *
	 * The prototype PMFS doesn't allow this when large pages are in
	 * use. It is not considered an error if this fails.
	 */
	util_range_none(pbp->addr, sizeof (struct pool_hdr));

	/* the data area should be kept read-only for debug version */
	RANGE_RO(pbp->data, pbp->datasize);

	return 0;

err:
	LOG(4, "error clean up");
	int oerrno = errno;
	if (locks)
		Free((void *)locks);
	if (bttp)
		btt_fini(bttp);
	errno = oerrno;
	return -1;
}

/*
 * pmemblk_create -- create a block memory pool
 */
PMEMblkpool *
pmemblk_create(const char *path, size_t bsize, size_t poolsize,
		mode_t mode)
{
	LOG(3, "path %s bsize %zu poolsize %zu mode %o",
			path, bsize, poolsize, mode);

	/* check if bsize is valid */
	if (bsize == 0) {
		ERR("Invalid block size %zu", bsize);
		errno = EINVAL;
		return NULL;
	}

	if (bsize > UINT32_MAX) {
		ERR("Invalid block size %zu", bsize);
		errno = EINVAL;
		return NULL;
	}

	struct pool_set *set;

	if (util_pool_create(&set, path, poolsize, PMEMBLK_MIN_POOL,
			roundup(sizeof (struct pmemblk), Pagesize),
			BLK_HDR_SIG, BLK_FORMAT_MAJOR,
			BLK_FORMAT_COMPAT, BLK_FORMAT_INCOMPAT,
			BLK_FORMAT_RO_COMPAT) != 0) {
		LOG(2, "cannot create pool or pool set");
		return NULL;
	}

	ASSERT(set->nreplicas > 0);

	struct pool_replica *rep = set->replica[0];
	PMEMblkpool *pbp = rep->part[0].addr;

	pbp->addr = pbp;
	pbp->size = rep->repsize;

	VALGRIND_REMOVE_PMEM_MAPPING(&pbp->addr,
			sizeof (struct pmemblk) -
			((uintptr_t)&pbp->addr - (uintptr_t)&pbp->hdr));

	if (set->nreplicas > 1) {
		ERR("replicas not supported");
		goto err;
	}

	/* create pool descriptor */
	if (pmemblk_descr_create(pbp, (uint32_t)bsize, set->zeroed) != 0) {
		LOG(2, "descriptor creation failed");
		goto err;
	}

	/* initialize runtime parts */
	if (pmemblk_runtime_init(pbp, bsize, 0, rep->is_pmem) != 0) {
		ERR("pool initialization failed");
		goto err;
	}

	if (util_poolset_chmod(set, mode))
		goto err;

	util_poolset_fdclose(set);

	util_poolset_free(set);

	LOG(3, "pbp %p", pbp);
	return pbp;

err:
	LOG(4, "error clean up");
	int oerrno = errno;
	util_poolset_close(set, 1);
	errno = oerrno;
	return NULL;
}


/*
 * pmemblk_open_common -- (internal) open a block memory pool
 *
 * This routine does all the work, but takes a cow flag so internal
 * calls can map a read-only pool if required.
 *
 * Passing in bsize == 0 means a valid pool header must exist (which
 * will supply the block size).
 */
static PMEMblkpool *
pmemblk_open_common(const char *path, size_t bsize, int cow)
{
	LOG(3, "path %s bsize %zu cow %d", path, bsize, cow);

	struct pool_set *set;

	if (util_pool_open(&set, path, cow, PMEMBLK_MIN_POOL,
			roundup(sizeof (struct pmemblk), Pagesize),
			BLK_HDR_SIG, BLK_FORMAT_MAJOR,
			BLK_FORMAT_COMPAT, BLK_FORMAT_INCOMPAT,
			BLK_FORMAT_RO_COMPAT) != 0) {
		LOG(2, "cannot open pool or pool set");
		return NULL;
	}

	ASSERT(set->nreplicas > 0);

	struct pool_replica *rep = set->replica[0];
	PMEMblkpool *pbp = rep->part[0].addr;

	pbp->addr = pbp;
	pbp->size = rep->repsize;

	VALGRIND_REMOVE_PMEM_MAPPING(&pbp->addr,
			sizeof (struct pmemblk) -
			((uintptr_t)&pbp->addr - (uintptr_t)&pbp->hdr));

	if (set->nreplicas > 1) {
		ERR("replicas not supported");
		goto err;
	}

	/* validate pool descriptor */
	if (pmemblk_descr_check(pbp, &bsize) != 0) {
		LOG(2, "descriptor check failed");
		goto err;
	}

	/* initialize runtime parts */
	if (pmemblk_runtime_init(pbp, bsize, set->rdonly, rep->is_pmem) != 0) {
		ERR("pool initialization failed");
		goto err;
	}

	util_poolset_fdclose(set);

	util_poolset_free(set);

	LOG(3, "pbp %p", pbp);
	return pbp;

err:
	LOG(4, "error clean up");
	int oerrno = errno;
	util_poolset_close(set, 0);
	errno = oerrno;
	return NULL;
}

/*
 * pmemblk_open -- open a block memory pool
 */
PMEMblkpool *
pmemblk_open(const char *path, size_t bsize)
{
	LOG(3, "path %s bsize %zu", path, bsize);

	return pmemblk_open_common(path, bsize, 0);
}

/*
 * pmemblk_close -- close a block memory pool
 */
void
pmemblk_close(PMEMblkpool *pbp)
{
	LOG(3, "pbp %p", pbp);

	btt_fini(pbp->bttp);
	if (pbp->locks) {
		for (unsigned i = 0; i < pbp->nlane; i++)
			pthread_mutex_destroy(&pbp->locks[i]);
		Free((void *)pbp->locks);
	}

#ifdef DEBUG
	/* destroy debug lock */
	pthread_mutex_destroy(&pbp->write_lock);
#endif

	VALGRIND_REMOVE_PMEM_MAPPING(pbp->addr, pbp->size);
	util_unmap(pbp->addr, pbp->size);
}

/*
 * pmemblk_bsize -- return size of block for specified pool
 */
size_t
pmemblk_bsize(PMEMblkpool *pbp)
{
	LOG(3, "pbp %p", pbp);

	return le32toh(pbp->bsize);
}

/*
 * pmemblk_nblock -- return number of usable blocks in a block memory pool
 */
size_t
pmemblk_nblock(PMEMblkpool *pbp)
{
	LOG(3, "pbp %p", pbp);

	return btt_nlba(pbp->bttp);
}

/*
 * pmemblk_read -- read a block in a block memory pool
 */
int
pmemblk_read(PMEMblkpool *pbp, void *buf, off_t blockno)
{
	LOG(3, "pbp %p buf %p blockno %lld", pbp, buf, (long long)blockno);

	if (blockno < 0) {
		ERR("negative block number");
		errno = EINVAL;
		return -1;
	}

	unsigned lane;

	if (lane_enter(pbp, &lane) < 0)
		return -1;

	int err = btt_read(pbp->bttp, lane, (uint64_t)blockno, buf);

	lane_exit(pbp, lane);

	return err;
}

/*
 * pmemblk_write -- write a block (atomically) in a block memory pool
 */
int
pmemblk_write(PMEMblkpool *pbp, const void *buf, off_t blockno)
{
	LOG(3, "pbp %p buf %p blockno %lld", pbp, buf, (long long)blockno);

	if (pbp->rdonly) {
		ERR("EROFS (pool is read-only)");
		errno = EROFS;
		return -1;
	}

	if (blockno < 0) {
		ERR("negative block number");
		errno = EINVAL;
		return -1;
	}

	unsigned lane;

	if (lane_enter(pbp, &lane) < 0)
		return -1;

	int err = btt_write(pbp->bttp, lane, (uint64_t)blockno, buf);

	lane_exit(pbp, lane);

	return err;
}

/*
 * pmemblk_set_zero -- zero a block in a block memory pool
 */
int
pmemblk_set_zero(PMEMblkpool *pbp, off_t blockno)
{
	LOG(3, "pbp %p blockno %lld", pbp, (long long)blockno);

	if (pbp->rdonly) {
		ERR("EROFS (pool is read-only)");
		errno = EROFS;
		return -1;
	}

	if (blockno < 0) {
		ERR("negative block number");
		errno = EINVAL;
		return -1;
	}

	unsigned lane;

	if (lane_enter(pbp, &lane) < 0)
		return -1;

	int err = btt_set_zero(pbp->bttp, lane, (uint64_t)blockno);

	lane_exit(pbp, lane);

	return err;
}

/*
 * pmemblk_set_error -- set the error state on a block in a block memory pool
 */
int
pmemblk_set_error(PMEMblkpool *pbp, off_t blockno)
{
	LOG(3, "pbp %p blockno %lld", pbp, (long long)blockno);

	if (pbp->rdonly) {
		ERR("EROFS (pool is read-only)");
		errno = EROFS;
		return -1;
	}

	if (blockno < 0) {
		ERR("negative block number");
		errno = EINVAL;
		return -1;
	}

	unsigned lane;

	if (lane_enter(pbp, &lane) < 0)
		return -1;

	int err = btt_set_error(pbp->bttp, lane, (uint64_t)blockno);

	lane_exit(pbp, lane);

	return err;
}

/*
 * pmemblk_check -- block memory pool consistency check
 */
int
pmemblk_check(const char *path, size_t bsize)
{
	LOG(3, "path \"%s\" bsize %zu", path, bsize);

	/* map the pool read-only */
	PMEMblkpool *pbp = pmemblk_open_common(path, bsize, 1);
	if (pbp == NULL)
		return -1;	/* errno set by pmemblk_open_common() */

	int retval = btt_check(pbp->bttp);
	int oerrno = errno;
	pmemblk_close(pbp);
	errno = oerrno;

	return retval;
}
