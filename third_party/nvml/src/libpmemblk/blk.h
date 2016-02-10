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
 * blk.h -- internal definitions for libpmem blk module
 */

#define	PMEMBLK_LOG_PREFIX "libpmemblk"
#define	PMEMBLK_LOG_LEVEL_VAR "PMEMBLK_LOG_LEVEL"
#define	PMEMBLK_LOG_FILE_VAR "PMEMBLK_LOG_FILE"

/* attributes of the blk memory pool format for the pool header */
#define	BLK_HDR_SIG "PMEMBLK"	/* must be 8 bytes including '\0' */
#define	BLK_FORMAT_MAJOR 1
#define	BLK_FORMAT_COMPAT 0x0000
#define	BLK_FORMAT_INCOMPAT 0x0000
#define	BLK_FORMAT_RO_COMPAT 0x0000

struct pmemblk {
	struct pool_hdr hdr;	/* memory pool header */

	/* root info for on-media format... */
	uint32_t bsize;			/* block size */

	/* flag indicating if the pool was zero-initialized */
	int is_zeroed;

	/* some run-time state, allocated out of memory pool... */
	void *addr;			/* mapped region */
	size_t size;			/* size of mapped region */
	int is_pmem;			/* true if pool is PMEM */
	int rdonly;			/* true if pool is opened read-only */
	void *data;			/* post-header data area */
	size_t datasize;		/* size of data area */
	size_t nlba;			/* number of LBAs in pool */
	struct btt *bttp;		/* btt handle */
	unsigned nlane;			/* number of lanes */
	unsigned next_lane;		/* used to rotate through lanes */
	pthread_mutex_t *locks;		/* one per lane */

#ifdef DEBUG
	/* held during read/write mprotected sections */
	pthread_mutex_t write_lock;
#endif
};

/* data area starts at this alignment after the struct pmemblk above */
#define	BLK_FORMAT_DATA_ALIGN ((uintptr_t)4096)
