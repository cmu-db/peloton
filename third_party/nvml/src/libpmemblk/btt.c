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
 * btt.c -- block translation table providing atomic block updates
 *
 * This is a user-space implementation of the BTT mechanism providing
 * single block powerfail write atomicity, as described by:
 *	The NVDIMM Namespace Specification
 *
 * To use this module, the caller must provide five routines for
 * accessing the namespace containing the data (in this context,
 * "namespace" refers to the storage containing the BTT layout, such
 * as a file).  All namespace I/O is done by these callbacks:
 *
 *	nsread	Read count bytes from namespace at offset off
 *	nswrite	Write count bytes to namespace at offset off
 *	nszero	Zero count bytes in namespace at offset off
 *	nsmap	Return direct access to a range of a namespace
 *	nssync	Flush changes made to an nsmap'd range
 *
 * Data written by the nswrite callback is flushed out to the media
 * (made durable) when the call returns.  Data written directly via
 * the nsmap callback must be flushed explicitly using nssync.
 *
 * The caller passes these callbacks, along with information such as
 * namespace size and UUID to btt_init() and gets back an opaque handle
 * which is then used with the rest of the entry points.
 *
 * Here is a brief list of the entry points to this module:
 *
 *	btt_nlane	Returns number of concurrent threads allowed
 *
 *	btt_nlba	Returns the usable size, as a count of LBAs
 *
 *	btt_read	Reads a single block at a given LBA
 *
 *	btt_write	Writes a single block (atomically) at a given LBA
 *
 *	btt_set_zero	Sets a block to read back as zeros
 *
 *	btt_set_error	Sets a block to return error on read
 *
 *	btt_check	Checks the BTT metadata for consistency
 *
 *	btt_fini	Frees run-time state, done using namespace
 *
 * If the caller is multi-threaded, it must only allow btt_nlane() threads
 * to enter this module at a time, each assigned a unique "lane" number
 * between 0 and btt_nlane() - 1.
 *
 * There are a number of static routines defined in this module.  Here's
 * a brief overview of the most important routines:
 *
 *	read_layout	Checks for valid BTT layout and builds run-time state.
 *			A number of helper functions are used by read_layout
 *			to handle various parts of the metadata:
 *				read_info
 *				read_arenas
 *				read_arena
 *				read_flogs
 *				read_flog_pair
 *
 *	write_layout	Generates a new BTT layout when one doesn't exist.
 *			Once a new layout is written, write_layout uses
 *			the same helper functions above to construct the
 *			run-time state.
 *
 *	invalid_lba	Range check done by each entry point that takes
 *			an LBA.
 *
 *	lba_to_arena_lba
 *			Find the arena and LBA in that arena for a given
 *			external LBA.  This is the heart of the arena
 *			range matching logic.
 *
 *	flog_update	Update the BTT free list/log combined data structure
 *			(known as the "flog").  This is the heart of the
 *			logic that makes writes powerfail atomic.
 *
 *	map_lock	These routines provide atomic access to the BTT map
 *	map_unlock	data structure in an area.
 *	map_abort
 *
 *	map_entry_setf	Common code for btt_set_zero() and btt_set_error().
 *
 *	zero_block	Generate a block of all zeros (instead of actually
 *			doing a read), when the metadata indicates the
 *			block should read as zeros.
 *
 *	build_rtt	These routines construct the run-time tracking
 *	build_map_locks	data structures used during I/O.
 */

#include <stdio.h>
#include <sys/param.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <endian.h>
#include <uuid/uuid.h>

#include "out.h"
#include "util.h"
#include "btt.h"
#include "btt_layout.h"

/*
 * The opaque btt handle containing state tracked by this module
 * for the btt namespace.  This is created by btt_init(), handed to
 * all the other btt_* entry points, and deleted by btt_fini().
 */
struct btt {
	unsigned nlane; /* number of concurrent threads allowed per btt */

	/*
	 * The laidout flag indicates whether the namespace contains valid BTT
	 * metadata.  It is initialized by read_layout() and if no valid layout
	 * is found, all reads return zeros and the first write will write the
	 * BTT layout.  The layout_write_mutex protects the laidout flag so
	 * only one write threads ends up writing the initial metadata by
	 * calling write_layout().
	 */
	pthread_mutex_t layout_write_mutex;
	int laidout;

	/*
	 * UUID of the BTT
	 */
	uint8_t uuid[BTTINFO_UUID_LEN];

	/*
	 * UUID of the containing namespace, used to validate BTT metadata.
	 */
	uint8_t parent_uuid[BTTINFO_UUID_LEN];

	/*
	 * Parameters controlling/describing the BTT layout.
	 */
	uint64_t rawsize;		/* size of containing namespace */
	uint32_t lbasize;		/* external LBA size */
	uint32_t nfree;			/* available flog entries */
	uint64_t nlba;			/* total number of external LBAs */
	unsigned narena;		/* number of arenas */

	/* run-time state kept for each arena */
	struct arena {
		uint32_t flags;		/* arena flags (btt_info) */
		uint32_t external_nlba;	/* LBAs that live in this arena */
		uint32_t internal_lbasize;
		uint32_t internal_nlba;

		/*
		 * The following offsets are relative to the beginning of
		 * the encapsulating namespace.  This is different from
		 * how these offsets are stored on-media, where they are
		 * relative to the start of the arena.  The offset are
		 * converted by read_layout() to make them more convenient
		 * for run-time use.
		 */
		uint64_t startoff;	/* offset to start of arena */
		uint64_t dataoff;	/* offset to arena data area */
		uint64_t mapoff;	/* offset to area map */
		uint64_t flogoff;	/* offset to area flog */
		uint64_t nextoff;	/* offset to next arena */

		/*
		 * Run-time flog state.  Indexed by lane.
		 *
		 * The write path uses the flog to find the free block
		 * it writes to before atomically making it the new
		 * active block for an external LBA.
		 *
		 * The read path doesn't use the flog at all.
		 */
		struct flog_runtime {
			struct btt_flog flog;	/* current info */
			uint64_t entries[2];	/* offsets for flog pair */
			int next;		/* next write (0 or 1) */
		} *flogs;

		/*
		 * Read tracking table.  Indexed by lane.
		 *
		 * Before using a free block found in the flog, the write path
		 * scans the rtt to see if there are any outstanding reads on
		 * that block (reads that started before the block was freed by
		 * a concurrent write).  Unused slots in the rtt are indicated
		 * by setting the error bit, BTT_MAP_ENTRY_ERROR, so that the
		 * entry won't match any post-map LBA when checked.
		 */
		uint32_t volatile *rtt;

		/*
		 * Map locking.  Indexed by pre-map LBA modulo nlane.
		 */
		pthread_mutex_t *map_locks;

		/*
		 * Arena info block locking.
		 */
		pthread_mutex_t info_lock;
	} *arenas;

	/*
	 * Callbacks for doing I/O to namespace.  These are provided by
	 * the code calling the BTT module, which passes them in to
	 * btt_init().  All namespace I/O is done using these.
	 *
	 * The opaque namespace handle "ns" was provided by the code calling
	 * the BTT module and is passed to each callback to identify the
	 * namespace being accessed.
	 */
	void *ns;
	const struct ns_callback *ns_cbp;
};

/*
 * Signature for arena info blocks.  Total size is 16 bytes, including
 * the '\0' added to the string by the declaration (the last two bytes
 * of the string are '\0').
 */
static const char Sig[] = "BTT_ARENA_INFO\0";

/*
 * Zeroed out flog entry, used when initializing the flog.
 */
static const struct btt_flog Zflog;

/*
 * Lookup table and macro for looking up sequence numbers.  These are
 * the 2-bit numbers that cycle between 01, 10, and 11.
 *
 * To advance a sequence number to the next number, use something like:
 *	seq = NSEQ(seq);
 */
static const unsigned Nseq[] = { 0, 2, 3, 1 };
#define	NSEQ(seq) (Nseq[(seq) & 3])

/*
 * invalid_lba -- (internal) set errno and return true if lba is invalid
 *
 * This function is used at the top of the entry points where an external
 * LBA is provided, like this:
 *
 *	if (invalid_lba(bttp, lba))
 *		return -1;
 */
static int
invalid_lba(struct btt *bttp, uint64_t lba)
{
	LOG(3, "bttp %p lba %ju", bttp, lba);

	if (lba >= bttp->nlba) {
		ERR("lba out of range (nlba %ju)", bttp->nlba);
		errno = EINVAL;
		return 1;
	}

	return 0;
}

/*
 * read_info -- (internal) convert btt_info to host byte order & validate
 *
 * Returns true if info block is valid, and all the integer fields are
 * converted to host byte order.  If the info block is not valid, this
 * routine returns false and the info block passed in is left in an
 * unknown state.
 */
static int
read_info(struct btt *bttp, struct btt_info *infop)
{
	LOG(3, "infop %p", infop);

	if (memcmp(infop->sig, Sig, BTTINFO_SIG_LEN)) {
		LOG(3, "signature invalid");
		return 0;
	}

	if (memcmp(infop->parent_uuid, bttp->parent_uuid, BTTINFO_UUID_LEN)) {
		LOG(3, "parent UUID mismatch");
		return 0;
	}

	/* to be valid, the fields must checksum correctly */
	if (!util_checksum(infop, sizeof (*infop), &infop->checksum, 0)) {
		LOG(3, "invalid checksum");
		return 0;
	}

	/* to be valid, info block must have a major version of at least 1 */
	if ((infop->major = le16toh(infop->major)) == 0) {
		LOG(3, "invalid major version (0)");
		return 0;
	}

	infop->flags = le32toh(infop->flags);
	infop->minor = le16toh(infop->minor);
	infop->external_lbasize = le32toh(infop->external_lbasize);
	infop->external_nlba = le32toh(infop->external_nlba);
	infop->internal_lbasize = le32toh(infop->internal_lbasize);
	infop->internal_nlba = le32toh(infop->internal_nlba);
	infop->nfree = le32toh(infop->nfree);
	infop->infosize = le32toh(infop->infosize);
	infop->nextoff = le64toh(infop->nextoff);
	infop->dataoff = le64toh(infop->dataoff);
	infop->mapoff = le64toh(infop->mapoff);
	infop->flogoff = le64toh(infop->flogoff);
	infop->infooff = le64toh(infop->infooff);

	return 1;
}

/*
 * map_entry_is_zero -- (internal) checks if map_entry is in zero state
 */
static inline int
map_entry_is_zero(uint32_t map_entry)
{
	return (map_entry & ~BTT_MAP_ENTRY_LBA_MASK) == BTT_MAP_ENTRY_ZERO;
}

/*
 * map_entry_is_error -- (internal) checks if map_entry is in error state
 */
static inline int
map_entry_is_error(uint32_t map_entry)
{
	return (map_entry & ~BTT_MAP_ENTRY_LBA_MASK) == BTT_MAP_ENTRY_ERROR;
}

/*
 * map_entry_is_initial -- (internal) checks if map_entry is in initial state
 */
static inline int
map_entry_is_initial(uint32_t map_entry)
{
	return (map_entry & ~BTT_MAP_ENTRY_LBA_MASK) == 0;
}

/*
 * map_entry_is_zero_or_initial -- (internal) checks if map_entry is in initial
 * or zero state
 */
static inline int
map_entry_is_zero_or_initial(uint32_t map_entry)
{
	uint32_t entry_flags = map_entry & ~BTT_MAP_ENTRY_LBA_MASK;
	return entry_flags == 0 || entry_flags == BTT_MAP_ENTRY_ZERO;
}

/*
 * read_flog_pair -- (internal) load up a single flog pair
 *
 * Zero is returned on success, otherwise -1/errno.
 */
static int
read_flog_pair(struct btt *bttp, unsigned lane, struct arena *arenap,
	uint64_t flog_off, struct flog_runtime *flog_runtimep, uint32_t flognum)
{
	LOG(5, "bttp %p lane %u arenap %p flog_off %ju runtimep %p "
		"flognum %u", bttp, lane, arenap, flog_off, flog_runtimep,
		flognum);

	flog_runtimep->entries[0] = flog_off;
	flog_runtimep->entries[1] = flog_off + sizeof (struct btt_flog);

	if (lane >= bttp->nfree) {
		ERR("invalid lane %u among nfree %d", lane, bttp->nfree);
		errno = EINVAL;
		return -1;
	}

	if (flog_off == 0) {
		ERR("invalid flog offset %ju", flog_off);
		errno = EINVAL;
		return -1;
	}

	struct btt_flog flog_pair[2];
	if ((*bttp->ns_cbp->nsread)(bttp->ns, lane, flog_pair,
				sizeof (flog_pair), flog_off) < 0)
		return -1;

	flog_pair[0].lba = le32toh(flog_pair[0].lba);
	flog_pair[0].old_map = le32toh(flog_pair[0].old_map);
	flog_pair[0].new_map = le32toh(flog_pair[0].new_map);
	flog_pair[0].seq = le32toh(flog_pair[0].seq);

	if (invalid_lba(bttp, flog_pair[0].lba))
		return -1;

	flog_pair[1].lba = le32toh(flog_pair[1].lba);
	flog_pair[1].old_map = le32toh(flog_pair[1].old_map);
	flog_pair[1].new_map = le32toh(flog_pair[1].new_map);
	flog_pair[1].seq = le32toh(flog_pair[1].seq);

	if (invalid_lba(bttp, flog_pair[1].lba))
		return -1;

	LOG(6, "flog_pair[0] flog_off %ju old_map %u new_map %u seq %u",
			flog_off, flog_pair[0].old_map,
			flog_pair[0].new_map, flog_pair[0].seq);
	LOG(6, "flog_pair[1] old_map %u new_map %u seq %u",
			flog_pair[1].old_map, flog_pair[1].new_map,
			flog_pair[1].seq);

	/*
	 * Interesting cases:
	 *	- no valid seq numbers:  layout consistency error
	 *	- one valid seq number:  that's the current entry
	 *	- two valid seq numbers: higher number is current entry
	 *	- identical seq numbers: layout consistency error
	 */
	struct btt_flog *currentp;
	if (flog_pair[0].seq == flog_pair[1].seq) {
		ERR("flog layout error: bad seq numbers %d %d",
				flog_pair[0].seq, flog_pair[1].seq);
		arenap->flags |= BTTINFO_FLAG_ERROR;
		return 0;
	} else if (flog_pair[0].seq == 0) {
		/* singleton valid flog at flog_pair[1] */
		currentp = &flog_pair[1];
		flog_runtimep->next = 0;
	} else if (flog_pair[1].seq == 0) {
		/* singleton valid flog at flog_pair[0] */
		currentp = &flog_pair[0];
		flog_runtimep->next = 1;
	} else if (NSEQ(flog_pair[0].seq) == flog_pair[1].seq) {
		/* flog_pair[1] has the later sequence number */
		currentp = &flog_pair[1];
		flog_runtimep->next = 0;
	} else {
		/* flog_pair[0] has the later sequence number */
		currentp = &flog_pair[0];
		flog_runtimep->next = 1;
	}

	LOG(6, "run-time flog next is %d", flog_runtimep->next);

	/* copy current flog into run-time flog state */
	flog_runtimep->flog = *currentp;

	LOG(9, "read flog[%u]: lba %u old %u%s%s%s new %u%s%s%s", flognum,
		currentp->lba,
		currentp->old_map & BTT_MAP_ENTRY_LBA_MASK,
		(map_entry_is_error(currentp->old_map)) ? " ERROR" : "",
		(map_entry_is_zero(currentp->old_map)) ? " ZERO" : "",
		(map_entry_is_initial(currentp->old_map)) ? " INIT" : "",
		currentp->new_map & BTT_MAP_ENTRY_LBA_MASK,
		(map_entry_is_error(currentp->new_map)) ? " ERROR" : "",
		(map_entry_is_zero(currentp->new_map)) ? " ZERO" : "",
		(map_entry_is_initial(currentp->new_map)) ? " INIT" : "");

	/*
	 * Decide if the current flog info represents a completed
	 * operation or an incomplete operation.  If completed, the
	 * old_map field will contain the free block to be used for
	 * the next write.  But if the operation didn't complete (indicated
	 * by the map entry not being updated), then the operation is
	 * completed now by updating the map entry.
	 *
	 * A special case, used by flog entries when first created, is
	 * when old_map == new_map.  This counts as a complete entry
	 * and doesn't require reading the map to see if recovery is
	 * required.
	 */
	if (currentp->old_map == currentp->new_map) {
		LOG(9, "flog[%u] entry complete (initial state)", flognum);
		return 0;
	}

	/* convert pre-map LBA into an offset into the map */
	uint64_t map_entry_off = arenap->mapoff +
				BTT_MAP_ENTRY_SIZE * currentp->lba;

	/* read current map entry */
	uint32_t entry;
	if ((*bttp->ns_cbp->nsread)(bttp->ns, lane, &entry,
				sizeof (entry), map_entry_off) < 0)
		return -1;

	entry = le32toh(entry);

	/* map entry in initial state */
	if (map_entry_is_initial(entry))
		entry = currentp->lba | BTT_MAP_ENTRY_NORMAL;

	if (currentp->new_map != entry && currentp->old_map == entry) {
		/* last update didn't complete */
		LOG(9, "recover flog[%u]: map[%u]: %u",
				flognum, currentp->lba, currentp->new_map);

		/*
		 * Recovery step is to complete the transaction by
		 * updating the map entry.
		 */
		entry = htole32(currentp->new_map);
		if ((*bttp->ns_cbp->nswrite)(bttp->ns, lane, &entry,
					sizeof (uint32_t), map_entry_off) < 0)
			return -1;
	}

	return 0;
}

/*
 * flog_update -- (internal) write out an updated flog entry
 *
 * The flog entries are not checksummed.  Instead, increasing sequence
 * numbers are used to atomically switch the active flog entry between
 * the first and second struct btt_flog in each slot.  In order for this
 * to work, the sequence number must be updated only after all the other
 * fields in the flog are updated.  So the writes to the flog are broken
 * into two writes, one for the first three fields (lba, old_map, new_map)
 * and, only after those fields are known to be written durably, the
 * second write for the seq field is done.
 *
 * Returns 0 on success, otherwise -1/errno.
 */
static int
flog_update(struct btt *bttp, unsigned lane, struct arena *arenap,
		uint32_t lba, uint32_t old_map, uint32_t new_map)
{
	LOG(3, "bttp %p lane %u arenap %p lba %u old_map %u new_map %u",
			bttp, lane, arenap, lba, old_map, new_map);

	/* construct new flog entry in little-endian byte order */
	struct btt_flog new_flog;
	new_flog.lba = htole32(lba);
	new_flog.old_map = htole32(old_map);
	new_flog.new_map = htole32(new_map);
	new_flog.seq = htole32(NSEQ(arenap->flogs[lane].flog.seq));

	uint64_t new_flog_off =
		arenap->flogs[lane].entries[arenap->flogs[lane].next];

	/* write out first two fields first */
	if ((*bttp->ns_cbp->nswrite)(bttp->ns, lane, &new_flog,
				sizeof (uint32_t) * 2, new_flog_off) < 0)
		return -1;
	new_flog_off += sizeof (uint32_t) * 2;

	/* write out new_map and seq field to make it active */
	if ((*bttp->ns_cbp->nswrite)(bttp->ns, lane, &new_flog.new_map,
				sizeof (uint32_t) * 2, new_flog_off) < 0)
		return -1;

	/* flog entry written successfully, update run-time state */
	arenap->flogs[lane].next = 1 - arenap->flogs[lane].next;
	arenap->flogs[lane].flog.lba = lba;
	arenap->flogs[lane].flog.old_map = old_map;
	arenap->flogs[lane].flog.new_map = new_map;
	arenap->flogs[lane].flog.seq = NSEQ(arenap->flogs[lane].flog.seq);

	LOG(9, "update flog[%u]: lba %u old %u%s%s%s new %u%s%s%s", lane, lba,
			old_map & BTT_MAP_ENTRY_LBA_MASK,
			(map_entry_is_error(old_map)) ? " ERROR" : "",
			(map_entry_is_zero(old_map)) ? " ZERO" : "",
			(map_entry_is_initial(old_map)) ? " INIT" : "",
			new_map & BTT_MAP_ENTRY_LBA_MASK,
			(map_entry_is_error(new_map)) ? " ERROR" : "",
			(map_entry_is_zero(new_map)) ? " ZERO" : "",
			(map_entry_is_initial(new_map)) ? " INIT" : "");

	return 0;
}

/*
 * arena_setf -- (internal) updates the given flag for the arena info block
 */
static int
arena_setf(struct btt *bttp, struct arena *arenap, unsigned lane, uint32_t setf)
{
	LOG(3, "bttp %p arenap %p lane %u setf 0x%x", bttp, arenap, lane, setf);

	/* update runtime state */
	__sync_fetch_and_or(&arenap->flags, setf);

	if (!bttp->laidout) {
		/* no layout yet to update */
		return 0;
	}

	/*
	 * Read, modify and write out the info block
	 * at both the beginning and end of the arena.
	 */
	uint64_t arena_off = arenap->startoff;

	struct btt_info info;

	/* protect from simultaneous writes to the layout */
	if ((errno = pthread_mutex_lock(&arenap->info_lock))) {
		ERR("!pthread_mutex_lock");
		return -1;
	}

	int oerrno;

	if ((*bttp->ns_cbp->nsread)(bttp->ns, lane, &info,
			sizeof (info), arena_off) < 0) {
		goto err;
	}

	uint64_t infooff = le64toh(info.infooff);

	/* update flags */
	info.flags |= htole32(setf);

	/* update checksum */
	util_checksum(&info, sizeof (info), &info.checksum, 1);

	if ((*bttp->ns_cbp->nswrite)(bttp->ns, lane, &info,
				sizeof (info), arena_off) < 0) {
		goto err;
	}

	if ((*bttp->ns_cbp->nswrite)(bttp->ns, lane, &info,
				sizeof (info), arena_off + infooff) < 0) {
		goto err;
	}

	oerrno = errno;
	if ((errno = pthread_mutex_unlock(&arenap->info_lock)))
		ERR("!pthread_mutex_unlock");
	errno = oerrno;
	return 0;

err:
	oerrno = errno;
	if ((errno = pthread_mutex_unlock(&arenap->info_lock)))
		ERR("!pthread_mutex_unlock");
	errno = oerrno;
	return -1;
}

/*
 * set_arena_error -- (internal) set the error flag for the given arena
 */
static int
set_arena_error(struct btt *bttp, struct arena *arenap, unsigned lane)
{
	LOG(3, "bttp %p arena %p lane %u", bttp, arenap, lane);

	return arena_setf(bttp, arenap, lane, BTTINFO_FLAG_ERROR);
}

/*
 * read_flogs -- (internal) load up all the flog entries for an arena
 *
 * Zero is returned on success, otherwise -1/errno.
 */
static int
read_flogs(struct btt *bttp, unsigned lane, struct arena *arenap)
{
	if ((arenap->flogs = Malloc(bttp->nfree *
			sizeof (struct flog_runtime))) == NULL) {
		ERR("!Malloc for %u flog entries", bttp->nfree);
		return -1;
	}
	memset(arenap->flogs, '\0', bttp->nfree * sizeof (struct flog_runtime));

	/*
	 * Load up the flog state.  read_flog_pair() will determine if
	 * any recovery steps are required take them on the in-memory
	 * data structures it creates. Sets error flag when it
	 * determines an invalid state.
	 */
	uint64_t flog_off = arenap->flogoff;
	struct flog_runtime *flog_runtimep = arenap->flogs;
	for (uint32_t i = 0; i < bttp->nfree; i++) {
		if (read_flog_pair(bttp, lane, arenap, flog_off,
						flog_runtimep, i) < 0) {
			set_arena_error(bttp, arenap, lane);
			return -1;
		}

		/* prepare for next time around the loop */
		flog_off += roundup(2 * sizeof (struct btt_flog),
				BTT_FLOG_PAIR_ALIGN);
		flog_runtimep++;
	}

	return 0;
}

/*
 * build_rtt -- (internal) construct a read tracking table for an arena
 *
 * Zero is returned on success, otherwise -1/errno.
 *
 * The rtt is big enough to hold an entry for each free block (nfree)
 * since nlane can't be bigger than nfree.  nlane may end up smaller,
 * in which case some of the high rtt entries will be unused.
 */
static int
build_rtt(struct btt *bttp, struct arena *arenap)
{
	if ((arenap->rtt = Malloc(bttp->nfree * sizeof (uint32_t)))
							== NULL) {
		ERR("!Malloc for %d rtt entries", bttp->nfree);
		return -1;
	}
	for (uint32_t lane = 0; lane < bttp->nfree; lane++)
		arenap->rtt[lane] = BTT_MAP_ENTRY_ERROR;
	__sync_synchronize();

	return 0;
}

/*
 * build_map_locks -- (internal) construct map locks
 *
 * Zero is returned on success, otherwise -1/errno.
 */
static int
build_map_locks(struct btt *bttp, struct arena *arenap)
{
	if ((arenap->map_locks =
			Malloc(bttp->nfree * sizeof (*arenap->map_locks)))
							== NULL) {
		ERR("!Malloc for %d map_lock entries", bttp->nfree);
		return -1;
	}
	for (uint32_t lane = 0; lane < bttp->nfree; lane++)
		pthread_mutex_init(&arenap->map_locks[lane], NULL);

	return 0;
}

/*
 * read_arena -- (internal) load up an arena and build run-time state
 *
 * Zero is returned on success, otherwise -1/errno.
 */
static int
read_arena(struct btt *bttp, unsigned lane, uint64_t arena_off,
		struct arena *arenap)
{
	LOG(3, "bttp %p lane %u arena_off %ju arenap %p",
			bttp, lane, arena_off, arenap);

	struct btt_info info;
	if ((*bttp->ns_cbp->nsread)(bttp->ns, lane, &info, sizeof (info),
							arena_off) < 0)
		return -1;

	arenap->flags = le32toh(info.flags);
	arenap->external_nlba = le32toh(info.external_nlba);
	arenap->internal_lbasize = le32toh(info.internal_lbasize);
	arenap->internal_nlba = le32toh(info.internal_nlba);

	arenap->startoff = arena_off;
	arenap->dataoff = arena_off + le64toh(info.dataoff);
	arenap->mapoff = arena_off + le64toh(info.mapoff);
	arenap->flogoff = arena_off + le64toh(info.flogoff);
	arenap->nextoff = arena_off + le64toh(info.nextoff);

	if (read_flogs(bttp, lane, arenap) < 0)
		return -1;

	if (build_rtt(bttp, arenap) < 0)
		return -1;

	if (build_map_locks(bttp, arenap) < 0)
		return -1;

	/* initialize the per arena info block lock */
	pthread_mutex_init(&arenap->info_lock, NULL);

	return 0;
}

/*
 * read_arenas -- (internal) load up all arenas and build run-time state
 *
 * On entry, layout must be known to be valid, and the number of arenas
 * must be known.  Zero is returned on success, otherwise -1/errno.
 */
static int
read_arenas(struct btt *bttp, unsigned lane, unsigned narena)
{
	LOG(3, "bttp %p lane %u narena %d", bttp, lane, narena);

	if ((bttp->arenas = Malloc(narena * sizeof (*bttp->arenas))) == NULL) {
		ERR("!Malloc for %u arenas", narena);
		goto err;
	}
	memset(bttp->arenas, '\0', narena * sizeof (*bttp->arenas));

	uint64_t arena_off = 0;
	struct arena *arenap = bttp->arenas;
	for (unsigned i = 0; i < narena; i++) {

		if (read_arena(bttp, lane, arena_off, arenap) < 0)
			goto err;

		/* prepare for next time around the loop */
		arena_off = arenap->nextoff;
		arenap++;
	}

	bttp->laidout = 1;

	return 0;

err:
	LOG(4, "error clean up");
	int oerrno = errno;
	if (bttp->arenas) {
		for (unsigned i = 0; i < bttp->narena; i++) {
			if (bttp->arenas[i].flogs)
				Free(bttp->arenas[i].flogs);
			if (bttp->arenas[i].rtt)
				Free((void *)bttp->arenas[i].rtt);
			if (bttp->arenas[i].map_locks)
				Free((void *)bttp->arenas[i].map_locks);
		}
		Free(bttp->arenas);
		bttp->arenas = NULL;
	}
	errno = oerrno;
	return -1;
}

/*
 * write_layout -- (internal) write out the initial btt metadata layout
 *
 * Called with write == 1 only once in the life time of a btt namespace, when
 * the first write happens.  The caller of this routine is responsible for
 * locking out multiple threads.  This routine doesn't read anything -- by the
 * time it is called, it is known there's no layout in the namespace and a new
 * layout should be written.
 *
 * Calling with write == 0 tells this routine to do the calculations for
 * bttp->narena and bttp->nlba, but don't write out any metadata.
 *
 * If successful, sets bttp->layout to 1 and returns 0.  Otherwise -1
 * is returned and errno is set, and bttp->layout remains 0 so that
 * later attempts to write will try again to create the layout.
 */
static int
write_layout(struct btt *bttp, unsigned lane, int write)
{
	LOG(3, "bttp %p lane %u write %d", bttp, lane, write);

	ASSERT(bttp->rawsize >= BTT_MIN_SIZE);
	ASSERT(bttp->nfree);

	/*
	 * If a new layout is being written, generate the BTT's UUID.
	 */
	if (write)
		uuid_generate(bttp->uuid);

	/*
	 * The number of arenas is the number of full arena of
	 * size BTT_MAX_ARENA that fit into rawsize and then, if
	 * the remainder is at least BTT_MIN_SIZE in size, then
	 * that adds one more arena.
	 */
	bttp->narena = (unsigned)(bttp->rawsize / BTT_MAX_ARENA);
	if (bttp->rawsize % BTT_MAX_ARENA >= BTT_MIN_SIZE)
		bttp->narena++;
	LOG(4, "narena %u", bttp->narena);

	uint64_t flog_size = bttp->nfree *
		roundup(2 * sizeof (struct btt_flog), BTT_FLOG_PAIR_ALIGN);
	flog_size = roundup(flog_size, BTT_ALIGNMENT);

	uint32_t internal_lbasize = bttp->lbasize;
	if (internal_lbasize < BTT_MIN_LBA_SIZE)
		internal_lbasize = BTT_MIN_LBA_SIZE;
	internal_lbasize =
		roundup(internal_lbasize, BTT_INTERNAL_LBA_ALIGNMENT);
	/* check for overflow */
	if (internal_lbasize < BTT_INTERNAL_LBA_ALIGNMENT) {
		errno = EINVAL;
		ERR("!Invalid lba size after alignment: %u ",
				internal_lbasize);
		return -1;
	}
	LOG(4, "adjusted internal_lbasize %u", internal_lbasize);

	uint64_t total_nlba = 0;
	uint64_t rawsize = bttp->rawsize;
	unsigned arena_num = 0;
	uint64_t arena_off = 0;

	/*
	 * for each arena...
	 */
	while (rawsize >= BTT_MIN_SIZE) {
		LOG(4, "layout arena %u", arena_num);

		uint64_t arena_rawsize = rawsize;
		if (arena_rawsize > BTT_MAX_ARENA) {
			arena_rawsize = BTT_MAX_ARENA;
		}
		rawsize -= arena_rawsize;
		arena_num++;

		uint64_t arena_datasize = arena_rawsize;
		arena_datasize -= 2 * sizeof (struct btt_info);
		arena_datasize -= flog_size;

		/* allow for map alignment padding */
		uint64_t internal_nlba = (arena_datasize - BTT_ALIGNMENT) /
			(internal_lbasize + BTT_MAP_ENTRY_SIZE);

		/* ensure the number of blocks is at least 2*nfree */
		if (internal_nlba < 2 * bttp->nfree) {
			errno = EINVAL;
			ERR("!number of internal blocks: %ju "
				"expected at least %u",
				internal_nlba, 2 * bttp->nfree);
			return -1;
		}
		ASSERT(internal_nlba <= UINT32_MAX);
		uint32_t internal_nlba_u32 = (uint32_t)internal_nlba;

		uint32_t external_nlba = internal_nlba_u32 - bttp->nfree;

		LOG(4, "internal_nlba %ju external_nlba %u",
				internal_nlba, external_nlba);

		total_nlba += external_nlba;

		/*
		 * The rest of the loop body calculates metadata structures
		 * and lays it out for this arena.  So only continue if
		 * the write flag is set.
		 */
		if (!write)
			continue;

		uint64_t mapsize = roundup(external_nlba * BTT_MAP_ENTRY_SIZE,
							BTT_ALIGNMENT);
		arena_datasize -= mapsize;

		ASSERT(arena_datasize / internal_lbasize >= internal_nlba);

		/*
		 * Calculate offsets for the BTT info block.  These are
		 * all relative to the beginning of the arena.
		 */
		uint64_t nextoff;
		if (rawsize >= BTT_MIN_SIZE)
			nextoff = arena_rawsize;
		else
			nextoff = 0;
		uint64_t infooff = arena_rawsize - sizeof (struct btt_info);
		uint64_t flogoff = infooff - flog_size;
		uint64_t mapoff = flogoff - mapsize;
		uint64_t dataoff = sizeof (struct btt_info);

		LOG(4, "nextoff 0x%016jx", nextoff);
		LOG(4, "dataoff 0x%016jx", dataoff);
		LOG(4, "mapoff  0x%016jx", mapoff);
		LOG(4, "flogoff 0x%016jx", flogoff);
		LOG(4, "infooff 0x%016jx", infooff);

		ASSERTeq(arena_datasize, mapoff - dataoff);

		/* zero map if ns is not zero-initialized */
		if (!bttp->ns_cbp->ns_is_zeroed) {
			if ((*bttp->ns_cbp->nszero)(bttp->ns, lane, mapsize,
					mapoff) < 0)
				return -1;
		}

		/* write out the initial flog */
		uint64_t flog_entry_off = arena_off + flogoff;
		uint32_t next_free_lba = external_nlba;
		for (uint32_t i = 0; i < bttp->nfree; i++) {
			struct btt_flog flog;
			flog.lba = 0;
			flog.old_map = flog.new_map =
				htole32(next_free_lba | BTT_MAP_ENTRY_ZERO);
			flog.seq = htole32(1);

			/*
			 * Write both btt_flog structs in the pair, writing
			 * the second one as all zeros.
			 */
			LOG(6, "flog[%u] entry off %ju initial %u + zero = %u",
					i, flog_entry_off,
					next_free_lba,
					next_free_lba | BTT_MAP_ENTRY_ZERO);
			if ((*bttp->ns_cbp->nswrite)(bttp->ns, lane, &flog,
					sizeof (flog), flog_entry_off) < 0)
				return -1;
			flog_entry_off += sizeof (flog);

			LOG(6, "flog[%u] entry off %ju zeros",
					i, flog_entry_off);
			if ((*bttp->ns_cbp->nswrite)(bttp->ns, lane, &Zflog,
					sizeof (Zflog), flog_entry_off) < 0)
				return -1;
			flog_entry_off += sizeof (flog);
			flog_entry_off = roundup(flog_entry_off,
					BTT_FLOG_PAIR_ALIGN);

			next_free_lba++;
		}

		/*
		 * Construct the BTT info block and write it out
		 * at both the beginning and end of the arena.
		 */
		struct btt_info info;
		memset(&info, '\0', sizeof (info));
		memcpy(info.sig, Sig, BTTINFO_SIG_LEN);
		memcpy(info.uuid, bttp->uuid, BTTINFO_UUID_LEN);
		memcpy(info.parent_uuid, bttp->parent_uuid, BTTINFO_UUID_LEN);
		info.major = htole16(BTTINFO_MAJOR_VERSION);
		info.minor = htole16(BTTINFO_MINOR_VERSION);
		info.external_lbasize = htole32(bttp->lbasize);
		info.external_nlba = htole32(external_nlba);
		info.internal_lbasize = htole32(internal_lbasize);
		info.internal_nlba = htole32(internal_nlba_u32);
		info.nfree = htole32(bttp->nfree);
		info.infosize = htole32(sizeof (info));
		info.nextoff = htole64(nextoff);
		info.dataoff = htole64(dataoff);
		info.mapoff = htole64(mapoff);
		info.flogoff = htole64(flogoff);
		info.infooff = htole64(infooff);

		util_checksum(&info, sizeof (info), &info.checksum, 1);

		if ((*bttp->ns_cbp->nswrite)(bttp->ns, lane, &info,
					sizeof (info), arena_off) < 0)
			return -1;
		if ((*bttp->ns_cbp->nswrite)(bttp->ns, lane, &info,
					sizeof (info), arena_off + infooff) < 0)
			return -1;

		arena_off += nextoff;
	}

	ASSERTeq(bttp->narena, arena_num);

	bttp->nlba = total_nlba;

	if (write) {
		/*
		 * The layout is written now, so load up the arenas.
		 */
		return read_arenas(bttp, lane, bttp->narena);
	}

	return 0;
}

/*
 * read_layout -- (internal) load up layout info from btt namespace
 *
 * Called once when the btt namespace is opened for use.
 * Sets bttp->layout to 0 if no valid layout is found, 1 otherwise.
 *
 * Any recovery actions required (as indicated by the flog state) are
 * performed by this routine.
 *
 * Any quick checks for layout consistency are performed by this routine
 * (quick enough to be done each time a BTT area is opened for use, not
 * like the slow consistency checks done by btt_check()).
 *
 * Returns 0 if no errors are encountered accessing the namespace (in this
 * context, detecting there's no layout is not an error if the nsread function
 * didn't have any problems doing the reads).  Otherwise, -1 is returned
 * and errno is set.
 */
static int
read_layout(struct btt *bttp, unsigned lane)
{
	LOG(3, "bttp %p", bttp);

	ASSERT(bttp->rawsize >= BTT_MIN_SIZE);

	unsigned narena = 0;
	uint32_t smallest_nfree = UINT32_MAX;
	uint64_t rawsize = bttp->rawsize;
	uint64_t total_nlba = 0;
	uint64_t arena_off = 0;

	bttp->nfree = BTT_DEFAULT_NFREE;

	/*
	 * For each arena, see if there's a valid info block
	 */
	while (rawsize >= BTT_MIN_SIZE) {
		narena++;

		struct btt_info info;
		if ((*bttp->ns_cbp->nsread)(bttp->ns, lane, &info,
					sizeof (info), arena_off) < 0)
			return -1;

		if (!read_info(bttp, &info)) {
			/*
			 * Failed to find complete BTT metadata.  Just
			 * calculate the narena and nlba values that will
			 * result when write_layout() gets called.  This
			 * allows checks against nlba to work correctly
			 * even before the layout is written.
			 */
			return write_layout(bttp, lane, 0);
		}
		if (info.external_lbasize != bttp->lbasize) {
			/* can't read it assuming the wrong block size */
			ERR("inconsistent lbasize");
			errno = EINVAL;
			return -1;
		}

		if (info.nfree == 0) {
			ERR("invalid nfree");
			errno = EINVAL;
			return -1;
		}

		if (info.external_nlba == 0) {
			ERR("invalid external_nlba");
			errno = EINVAL;
			return -1;
		}

		if (info.nextoff && (info.nextoff != BTT_MAX_ARENA)) {
			ERR("invalid arena size");
			errno = EINVAL;
			return -1;
		}

		if (info.nfree < smallest_nfree)
			smallest_nfree = info.nfree;

		total_nlba += info.external_nlba;
		arena_off += info.nextoff;
		if (info.nextoff == 0)
			break;
		if (info.nextoff > rawsize) {
			ERR("invalid next arena offset");
			errno = EINVAL;
			return -1;
		}
		rawsize -= info.nextoff;
	}

	ASSERT(narena);

	bttp->narena = narena;
	bttp->nlba = total_nlba;

	/*
	 * All arenas were valid.  nfree should be the smallest value found
	 * among different arenas.
	 */
	if (smallest_nfree < bttp->nfree)
		bttp->nfree = smallest_nfree;

	/*
	 * Load up arenas.
	 */
	return read_arenas(bttp, lane, narena);
}

/*
 * zero_block -- (internal) satisfy a read with a block of zeros
 *
 * Returns 0 on success, otherwise -1/errno.
 */
static int
zero_block(struct btt *bttp, void *buf)
{
	LOG(3, "bttp %p", bttp);

	memset(buf, '\0', bttp->lbasize);
	return 0;
}

/*
 * lba_to_arena_lba -- (internal) calculate the arena & pre-map LBA
 *
 * This routine takes the external LBA and matches it to the
 * appropriate arena, adjusting the lba for use within that arena.
 *
 * If successful, zero is returned, *arenapp is a pointer to the appropriate
 * arena struct in the run-time state, and *premap_lbap is the LBA adjusted
 * to an arena-internal LBA (also known as the pre-map LBA).  Otherwise
 * -1/errno.
 */
static int
lba_to_arena_lba(struct btt *bttp, uint64_t lba,
		struct arena **arenapp, uint32_t *premap_lbap)
{
	LOG(3, "bttp %p lba %ju", bttp, lba);

	ASSERT(bttp->laidout);

	unsigned arena;
	for (arena = 0; arena < bttp->narena; arena++)
		if (lba < bttp->arenas[arena].external_nlba)
			break;
		else
			lba -= bttp->arenas[arena].external_nlba;

	ASSERT(arena < bttp->narena);

	*arenapp = &bttp->arenas[arena];
	ASSERT(lba <= UINT32_MAX);
	*premap_lbap = (uint32_t)lba;

	LOG(3, "arenap %p pre-map LBA %u", *arenapp, *premap_lbap);
	return 0;
}

/*
 * btt_init -- prepare a btt namespace for use, returning an opaque handle
 *
 * Returns handle on success, otherwise NULL/errno.
 *
 * When submitted a pristine namespace it will be formatted implicitly when
 * touched for the first time.
 *
 * If arenas have different nfree values, we will be using the lowest one
 * found as limiting to the overall "bandwidth".
 */
struct btt *
btt_init(uint64_t rawsize, uint32_t lbasize, uint8_t parent_uuid[],
		unsigned maxlane, void *ns, const struct ns_callback *ns_cbp)
{
	LOG(3, "rawsize %ju lbasize %u", rawsize, lbasize);

	if (rawsize < BTT_MIN_SIZE) {
		ERR("rawsize smaller than BTT_MIN_SIZE %u", BTT_MIN_SIZE);
		errno = EINVAL;
		return NULL;
	}

	struct btt *bttp = Malloc(sizeof (*bttp));

	if (bttp == NULL) {
		ERR("!Malloc %zu bytes", sizeof (*bttp));
		return NULL;
	}

	memset(bttp, '\0', sizeof (*bttp));

	pthread_mutex_init(&bttp->layout_write_mutex, NULL);
	memcpy(bttp->parent_uuid, parent_uuid, BTTINFO_UUID_LEN);
	bttp->rawsize = rawsize;
	bttp->lbasize = lbasize;
	bttp->ns = ns;
	bttp->ns_cbp = ns_cbp;

	/*
	 * Load up layout, if it exists.
	 *
	 * Whether read_layout() finds a valid layout or not, it finishes
	 * updating these layout-related fields:
	 *	bttp->nfree
	 *	bttp->nlba
	 *	bttp->narena
	 * since these fields are used even before a valid layout it written.
	 */
	if (read_layout(bttp, 0) < 0) {
		btt_fini(bttp);		/* free up any allocations */
		return NULL;
	}

	bttp->nlane = bttp->nfree;

	/* maxlane, if provided, is an upper bound on nlane */
	if (maxlane && bttp->nlane > maxlane)
		bttp->nlane = maxlane;

	LOG(3, "success, bttp %p nlane %u", bttp, bttp->nlane);
	return bttp;
}

/*
 * btt_nlane -- return the number of "lanes" for this btt namespace
 *
 * The number of lanes is the number of threads allowed in this module
 * concurrently for a given btt.  Each thread executing this code must
 * have a unique "lane" number assigned to it between 0 and btt_nlane() - 1.
 */
unsigned
btt_nlane(struct btt *bttp)
{
	LOG(3, "bttp %p", bttp);

	return bttp->nlane;
}

/*
 * btt_nlba -- return the number of usable blocks in a btt namespace
 *
 * Valid LBAs to pass to btt_read() and btt_write() are 0 through
 * btt_nlba() - 1.
 */
size_t
btt_nlba(struct btt *bttp)
{
	LOG(3, "bttp %p", bttp);

	return bttp->nlba;
}

/*
 * btt_read -- read a block from a btt namespace
 *
 * Returns 0 on success, otherwise -1/errno.
 */
int
btt_read(struct btt *bttp, unsigned lane, uint64_t lba, void *buf)
{
	LOG(3, "bttp %p lane %u lba %ju", bttp, lane, lba);

	if (invalid_lba(bttp, lba))
		return -1;

	/* if there's no layout written yet, all reads come back as zeros */
	if (!bttp->laidout)
		return zero_block(bttp, buf);

	/* find which arena LBA lives in, and the offset to the map entry */
	struct arena *arenap;
	uint32_t premap_lba;
	uint64_t map_entry_off;
	if (lba_to_arena_lba(bttp, lba, &arenap, &premap_lba) < 0)
		return -1;

	/* convert pre-map LBA into an offset into the map */
	map_entry_off = arenap->mapoff + BTT_MAP_ENTRY_SIZE * premap_lba;

	/*
	 * Read the current map entry to get the post-map LBA for the data
	 * block read.
	 */
	uint32_t entry;

	if ((*bttp->ns_cbp->nsread)(bttp->ns, lane, &entry,
				sizeof (entry), map_entry_off) < 0)
		return -1;

	entry = le32toh(entry);

	/*
	 * Retries come back to the top of this loop (for a rare case where
	 * the map is changed by another thread doing writes to the same LBA).
	 */
	while (1) {
		if (map_entry_is_error(entry)) {
			ERR("EIO due to map entry error flag");
			errno = EIO;
			return -1;
		}

		if (map_entry_is_zero_or_initial(entry))
			return zero_block(bttp, buf);

		/*
		 * Record the post-map LBA in the read tracking table during
		 * the read.  The write will check entries in the read tracking
		 * table before allocating a block for a write, waiting for
		 * outstanding reads on that block to complete.
		 *
		 * Since we already checked for error, zero, and initial
		 * states above, the entry must have both error and zero
		 * bits set at this point (BTT_MAP_ENTRY_NORMAL).  We store
		 * the entry that way, with those bits set, in the rtt and
		 * btt_write() will check for it the same way, with the bits
		 * both set.
		 */
		arenap->rtt[lane] = entry;
		__sync_synchronize();

		/*
		 * In case this thread was preempted between reading entry and
		 * storing it in the rtt, check to see if the map changed.  If
		 * it changed, the block about to be read is at least free now
		 * (in the flog, but that's okay since the data will still be
		 * undisturbed) and potentially allocated and being used for
		 * another write (data disturbed, so not okay to continue).
		 */
		uint32_t latest_entry;
		if ((*bttp->ns_cbp->nsread)(bttp->ns, lane, &latest_entry,
				sizeof (latest_entry), map_entry_off) < 0) {
			arenap->rtt[lane] = BTT_MAP_ENTRY_ERROR;
			return -1;
		}

		latest_entry = le32toh(latest_entry);

		if (entry == latest_entry)
			break;			/* map stayed the same */
		else
			entry = latest_entry;	/* try again */
	}

	/*
	 * It is safe to read the block now, since the rtt protects the
	 * block from getting re-allocated to something else by a write.
	 */
	uint64_t data_block_off =
		arenap->dataoff + (entry & BTT_MAP_ENTRY_LBA_MASK) *
		arenap->internal_lbasize;
	int readret = (*bttp->ns_cbp->nsread)(bttp->ns, lane, buf,
					bttp->lbasize, data_block_off);

	/* done with read, so clear out rtt entry */
	arenap->rtt[lane] = BTT_MAP_ENTRY_ERROR;

	return readret;
}

/*
 * map_lock -- (internal) grab the map_lock and read a map entry
 */
static int
map_lock(struct btt *bttp, unsigned lane, struct arena *arenap,
		uint32_t *entryp, uint32_t premap_lba)
{
	LOG(3, "bttp %p lane %u arenap %p premap_lba %u",
			bttp, lane, arenap, premap_lba);

	uint64_t map_entry_off =
			arenap->mapoff + BTT_MAP_ENTRY_SIZE * premap_lba;

	/*
	 * map_locks[] contains nfree locks which are used to protect the map
	 * from concurrent access to the same cache line.  The index into
	 * map_locks[] is calculated by looking at the byte offset into the map
	 * (premap_lba * BTT_MAP_ENTRY_SIZE), figuring out how many cache lines
	 * that is into the map that is (dividing by BTT_MAP_LOCK_ALIGN), and
	 * then selecting one of nfree locks (the modulo at the end).
	 */
	uint32_t map_lock_num = premap_lba * BTT_MAP_ENTRY_SIZE /
			BTT_MAP_LOCK_ALIGN % bttp->nfree;
	if ((errno = pthread_mutex_lock(&arenap->map_locks[map_lock_num]))) {
		ERR("!pthread_mutex_lock");
		return -1;
	}

	/* read the old map entry */
	if ((*bttp->ns_cbp->nsread)(bttp->ns, lane, entryp,
				sizeof (uint32_t), map_entry_off) < 0) {
		int oerrno = errno;
		if ((errno = pthread_mutex_unlock(
					&arenap->map_locks[map_lock_num])))
			ERR("!pthread_mutex_unlock");
		errno = oerrno;
		return -1;
	}

	/* if map entry is in its initial state return premap_lba */
	if (map_entry_is_initial(*entryp))
		*entryp = htole32(premap_lba | BTT_MAP_ENTRY_NORMAL);

	LOG(9, "locked map[%d]: %u%s%s", premap_lba,
			*entryp & BTT_MAP_ENTRY_LBA_MASK,
			(map_entry_is_error(*entryp)) ? " ERROR" : "",
			(map_entry_is_zero(*entryp)) ? " ZERO" : "");

	return 0;
}

/*
 * map_abort -- (internal) drop the map_lock without updating the entry
 */
static void
map_abort(struct btt *bttp, unsigned lane, struct arena *arenap,
		uint32_t premap_lba)
{
	LOG(3, "bttp %p lane %u arenap %p premap_lba %u",
			bttp, lane, arenap, premap_lba);

	uint32_t map_lock_num = premap_lba * BTT_MAP_ENTRY_SIZE /
			BTT_MAP_LOCK_ALIGN % bttp->nfree;
	int oerrno = errno;
	if ((errno = pthread_mutex_unlock(&arenap->map_locks[map_lock_num])))
		ERR("!pthread_mutex_unlock");
	errno = oerrno;
}

/*
 * map_unlock -- (internal) update the map and drop the map_lock
 */
static int
map_unlock(struct btt *bttp, unsigned lane, struct arena *arenap,
		uint32_t entry, uint32_t premap_lba)
{
	LOG(3, "bttp %p lane %u arenap %p entry %u premap_lba %u",
			bttp, lane, arenap, entry, premap_lba);

	uint64_t map_entry_off =
			arenap->mapoff + BTT_MAP_ENTRY_SIZE * premap_lba;

	/* write the new map entry */
	int err = (*bttp->ns_cbp->nswrite)(bttp->ns, lane, &entry,
				sizeof (uint32_t), map_entry_off);

	uint32_t map_lock_num =
			premap_lba * BTT_MAP_ENTRY_SIZE / BTT_MAP_LOCK_ALIGN
			% bttp->nfree;

	int oerrno = errno;
	if ((errno = pthread_mutex_unlock(&arenap->map_locks[map_lock_num])))
		ERR("!pthread_mutex_unlock");
	errno = oerrno;

	LOG(9, "unlocked map[%d]: %u%s%s", premap_lba,
			entry & BTT_MAP_ENTRY_LBA_MASK,
			(map_entry_is_error(entry)) ? " ERROR" : "",
			(map_entry_is_zero(entry)) ? " ZERO" : "");

	return err;
}

/*
 * btt_write -- write a block to a btt namespace
 *
 * Returns 0 on success, otherwise -1/errno.
 */
int
btt_write(struct btt *bttp, unsigned lane, uint64_t lba, const void *buf)
{
	LOG(3, "bttp %p lane %u lba %ju", bttp, lane, lba);

	if (invalid_lba(bttp, lba))
		return -1;

	/* first write through here will initialize the metadata layout */
	if (!bttp->laidout) {
		int err = 0;

		if ((errno = pthread_mutex_lock(&bttp->layout_write_mutex))) {
			ERR("!pthread_mutex_lock");
			return -1;
		}
		if (!bttp->laidout)
			err = write_layout(bttp, lane, 1);

		int oerrno = errno;
		if ((errno = pthread_mutex_unlock(&bttp->layout_write_mutex)))
			ERR("!pthread_mutex_unlock");
		errno = oerrno;

		if (err < 0)
			return err;
	}

	/* find which arena LBA lives in, and the offset to the map entry */
	struct arena *arenap;
	uint32_t premap_lba;
	if (lba_to_arena_lba(bttp, lba, &arenap, &premap_lba) < 0)
		return -1;

	/* if the arena is in an error state, writing is not allowed */
	if (arenap->flags & BTTINFO_FLAG_ERROR_MASK) {
		ERR("EIO due to btt_info error flags 0x%x",
			arenap->flags & BTTINFO_FLAG_ERROR_MASK);
		errno = EIO;
		return -1;
	}

	/*
	 * This routine was passed a unique "lane" which is an index
	 * into the flog.  That means the free block held by flog[lane]
	 * is assigned to this thread and to no other threads (no additional
	 * locking required).  So start by performing the write to the
	 * free block.  It is only safe to write to a free block if it
	 * doesn't appear in the read tracking table, so scan that first
	 * and if found, wait for the thread reading from it to finish.
	 */
	uint32_t free_entry = (arenap->flogs[lane].flog.old_map &
			BTT_MAP_ENTRY_LBA_MASK) | BTT_MAP_ENTRY_NORMAL;

	LOG(3, "free_entry %u (before mask %u)", free_entry,
				arenap->flogs[lane].flog.old_map);

	/* wait for other threads to finish any reads on free block */
	for (unsigned i = 0; i < bttp->nlane; i++)
		while (arenap->rtt[i] == free_entry)
			;

	/* it is now safe to perform write to the free block */
	uint64_t data_block_off = arenap->dataoff +
		(free_entry & BTT_MAP_ENTRY_LBA_MASK) *
		arenap->internal_lbasize;
	if ((*bttp->ns_cbp->nswrite)(bttp->ns, lane, buf,
				bttp->lbasize, data_block_off) < 0)
		return -1;

	/*
	 * Make the new block active atomically by updating the on-media flog
	 * and then updating the map.
	 */
	uint32_t old_entry;
	if (map_lock(bttp, lane, arenap, &old_entry, premap_lba) < 0)
		return -1;

	old_entry = le32toh(old_entry);

	/* update the flog */
	if (flog_update(bttp, lane, arenap, premap_lba,
					old_entry, free_entry) < 0) {
		map_abort(bttp, lane, arenap, premap_lba);
		return -1;
	}

	if (map_unlock(bttp, lane, arenap, htole32(free_entry),
					premap_lba) < 0) {
		/*
		 * A critical write error occurred, set the arena's
		 * info block error bit.
		 */
		set_arena_error(bttp, arenap, lane);
		errno = EIO;
		return -1;
	}

	return 0;
}

/*
 * map_entry_setf -- (internal) set a given flag on a map entry
 *
 * Returns 0 on success, otherwise -1/errno.
 */
static int
map_entry_setf(struct btt *bttp, unsigned lane, uint64_t lba, uint32_t setf)
{
	LOG(3, "bttp %p lane %u lba %ju setf 0x%x", bttp, lane, lba, setf);

	if (invalid_lba(bttp, lba))
		return -1;

	if (!bttp->laidout) {
		/*
		 * No layout is written yet.  If the flag being set
		 * is the zero flag, it is superfluous since all blocks
		 * read as zero at this point.
		 */
		if (setf == BTT_MAP_ENTRY_ZERO)
			return 0;

		/*
		 * Treat this like the first write and write out
		 * the metadata layout at this point.
		 */
		int err = 0;
		if ((errno = pthread_mutex_lock(&bttp->layout_write_mutex))) {
			ERR("!pthread_mutex_lock");
			return -1;
		}
		if (!bttp->laidout)
			err = write_layout(bttp, lane, 1);

		int oerrno = errno;
		if ((errno = pthread_mutex_unlock(&bttp->layout_write_mutex)))
			ERR("!pthread_mutex_unlock");
		errno = oerrno;

		if (err < 0)
			return err;
	}

	/* find which arena LBA lives in, and the offset to the map entry */
	struct arena *arenap;
	uint32_t premap_lba;
	if (lba_to_arena_lba(bttp, lba, &arenap, &premap_lba) < 0)
		return -1;

	/* if the arena is in an error state, writing is not allowed */
	if (arenap->flags & BTTINFO_FLAG_ERROR_MASK) {
		ERR("EIO due to btt_info error flags 0x%x",
			arenap->flags & BTTINFO_FLAG_ERROR_MASK);
		errno = EIO;
		return -1;
	}

	/*
	 * Set the flags in the map entry.  To do this, read the
	 * current map entry, set the flags, and write out the update.
	 */
	uint32_t old_entry;
	uint32_t new_entry;

	if (map_lock(bttp, lane, arenap, &old_entry, premap_lba) < 0)
		return -1;

	old_entry = le32toh(old_entry);

	if (setf == BTT_MAP_ENTRY_ZERO &&
			map_entry_is_zero_or_initial(old_entry)) {
		map_abort(bttp, lane, arenap, premap_lba);
		return 0;	/* block already zero, nothing to do */
	}

	/* create the new map entry */
	new_entry = (old_entry & BTT_MAP_ENTRY_LBA_MASK) | setf;

	if (map_unlock(bttp, lane, arenap, htole32(new_entry), premap_lba) < 0)
		return -1;

	return 0;
}

/*
 * btt_set_zero -- mark a block as zeroed in a btt namespace
 *
 * Returns 0 on success, otherwise -1/errno.
 */
int
btt_set_zero(struct btt *bttp, unsigned lane, uint64_t lba)
{
	LOG(3, "bttp %p lane %u lba %ju", bttp, lane, lba);

	return map_entry_setf(bttp, lane, lba, BTT_MAP_ENTRY_ZERO);
}

/*
 * btt_set_error -- mark a block as in an error state in a btt namespace
 *
 * Returns 0 on success, otherwise -1/errno.
 */
int
btt_set_error(struct btt *bttp, unsigned lane, uint64_t lba)
{
	LOG(3, "bttp %p lane %u lba %ju", bttp, lane, lba);

	return map_entry_setf(bttp, lane, lba, BTT_MAP_ENTRY_ERROR);
}

/*
 * check_arena -- (internal) perform a consistency check on an arena
 */
static int
check_arena(struct btt *bttp, struct arena *arenap)
{
	LOG(3, "bttp %p arenap %p", bttp, arenap);

	int consistent = 1;

	uint64_t map_entry_off = arenap->mapoff;
	uint32_t bitmapsize = howmany(arenap->internal_nlba, 8);
	uint8_t *bitmap = Malloc(bitmapsize);
	if (bitmap == NULL) {
		ERR("!Malloc for bitmap");
		return -1;
	}
	memset(bitmap, '\0', bitmapsize);

	/*
	 * Go through every post-map LBA mentioned in the map and make sure
	 * there are no duplicates.  bitmap is used to track which LBAs have
	 * been seen so far.
	 */
	uint32_t *mapp = NULL;
	ssize_t mlen;
	int next_index = 0;
	size_t remaining = 0;
	for (uint32_t i = 0; i < arenap->external_nlba; i++) {
		uint32_t entry;

		if (remaining == 0) {
			/* request a mapping of remaining map area */
			size_t req_len =
				(arenap->external_nlba - i) * sizeof (uint32_t);
			mlen = (*bttp->ns_cbp->nsmap)(bttp->ns, 0,
				(void **)&mapp, req_len, map_entry_off);

			if (mlen < 0)
				return -1;

			remaining = (size_t)mlen;
			next_index = 0;
		}
		entry = le32toh(mapp[next_index]);

		/* for debug, dump non-zero map entries at log level 11 */
		if (map_entry_is_zero_or_initial(entry) == 0)
			LOG(11, "map[%d]: %u%s", i,
				entry &	BTT_MAP_ENTRY_LBA_MASK,
				(map_entry_is_error(entry)) ? " ERROR" : "");

		/* this is an uninitialized map entry, set the default value */
		if (map_entry_is_initial(entry))
			entry = i;
		else
			entry &= BTT_MAP_ENTRY_LBA_MASK;

		/* check if entry is valid */
		if (entry >= arenap->internal_nlba) {
			ERR("map[%d] entry out of bounds: %u", i, entry);
			errno = EINVAL;
			return -1;
		}

		if (util_isset(bitmap, entry)) {
			ERR("map[%d] duplicate entry: %u", i, entry);
			consistent = 0;
		} else
			util_setbit(bitmap, entry);

		map_entry_off += sizeof (uint32_t);
		next_index++;
		ASSERT(remaining >= sizeof (uint32_t));
		remaining -= sizeof (uint32_t);
	}

	/*
	 * Go through the free blocks in the flog, adding them to bitmap
	 * and checking for duplications.  It is sufficient to read the
	 * run-time flog here, avoiding more calls to nsread.
	 */
	for (uint32_t i = 0; i < bttp->nfree; i++) {
		uint32_t entry = arenap->flogs[i].flog.old_map;
		entry &= BTT_MAP_ENTRY_LBA_MASK;

		if (util_isset(bitmap, entry)) {
			ERR("flog[%u] duplicate entry: %u", i, entry);
			consistent = 0;
		} else
			util_setbit(bitmap, entry);
	}

	/*
	 * Make sure every possible post-map LBA was accounted for
	 * in the two loops above.
	 */
	for (uint32_t i = 0; i < arenap->internal_nlba; i++)
		if (util_isclr(bitmap, i)) {
			ERR("unreferenced lba: %d", i);
			consistent = 0;
		}


	Free(bitmap);

	return consistent;
}

/*
 * btt_check -- perform a consistency check on a btt namespace
 *
 * This routine contains a fairly high-impact set of consistency checks.
 * It may use a good amount of dynamic memory and CPU time performing
 * the checks.  Any lightweight, quick consistency checks are included
 * in read_layout() so they happen every time the BTT area is opened
 * for use.
 *
 * Returns true if consistent, zero if inconsistent, -1/error if checking
 * cannot happen due to other errors.
 *
 * No lane number required here because only one thread is allowed -- all
 * other threads must be locked out of all btt routines for this btt
 * namespace while this is running.
 */
int
btt_check(struct btt *bttp)
{
	LOG(3, "bttp %p", bttp);

	int consistent = 1;

	if (!bttp->laidout) {
		/* consistent by definition */
		LOG(3, "no layout yet");
		return consistent;
	}

	/* XXX report issues found during read_layout (from flags) */

	/* for each arena... */
	struct arena *arenap = bttp->arenas;
	for (unsigned i = 0; i < bttp->narena; i++, arenap++) {
		/*
		 * Perform the consistency checks for the arena.
		 */
		int retval = check_arena(bttp, arenap);
		if (retval < 0)
			return retval;
		else if (retval == 0)
			consistent = 0;
	}

	/* XXX stub */
	return consistent;
}

/*
 * btt_fini -- delete opaque btt info, done using btt namespace
 */
void
btt_fini(struct btt *bttp)
{
	LOG(3, "bttp %p", bttp);

	if (bttp->arenas) {
		for (unsigned i = 0; i < bttp->narena; i++) {
			if (bttp->arenas[i].flogs)
				Free(bttp->arenas[i].flogs);
			if (bttp->arenas[i].rtt)
				Free((void *)bttp->arenas[i].rtt);
			if (bttp->arenas[i].rtt)
				Free((void *)bttp->arenas[i].map_locks);
		}
		Free(bttp->arenas);
	}
	Free(bttp);
}
