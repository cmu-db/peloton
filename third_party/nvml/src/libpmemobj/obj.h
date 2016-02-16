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
 * obj.h -- internal definitions for obj module
 */

#include <stddef.h>

#define	PMEMOBJ_LOG_PREFIX "libpmemobj"
#define	PMEMOBJ_LOG_LEVEL_VAR "PMEMOBJ_LOG_LEVEL"
#define	PMEMOBJ_LOG_FILE_VAR "PMEMOBJ_LOG_FILE"

/* attributes of the obj memory pool format for the pool header */
#define	OBJ_HDR_SIG "PMEMOBJ"	/* must be 8 bytes including '\0' */
#define	OBJ_FORMAT_MAJOR 1
#define	OBJ_FORMAT_COMPAT 0x0000
#define	OBJ_FORMAT_INCOMPAT 0x0000
#define	OBJ_FORMAT_RO_COMPAT 0x0000

/* size of the persistent part of PMEMOBJ pool descriptor (2kB) */
#define	OBJ_DSC_P_SIZE		2048
/* size of unused part of the persistent part of PMEMOBJ pool descriptor */
#define	OBJ_DSC_P_UNUSED	(OBJ_DSC_P_SIZE - PMEMOBJ_MAX_LAYOUT - 56)

#define	OBJ_LANES_OFFSET	8192	/* lanes offset (8kB) */
#define	OBJ_NLANES		1024	/* number of lanes */

#define	MAX_CACHED_RANGE_SIZE 32
#define	MAX_CACHED_RANGES 127 /* calculated to be exactly 8192 bytes */

#define	OBJ_OOB_SIZE		(sizeof (struct oob_header))
#define	OBJ_OFF_TO_PTR(pop, off) ((void *)((uintptr_t)(pop) + (off)))
#define	OBJ_PTR_TO_OFF(pop, ptr) ((uintptr_t)(ptr) - (uintptr_t)(pop))
#define	OBJ_OID_IS_NULL(oid)	((oid).off == 0)
#define	OBJ_LIST_EMPTY(head)	OBJ_OID_IS_NULL((head)->pe_first)
#define	OBJ_OFF_FROM_HEAP(pop, off)\
	((off) >= (pop)->heap_offset &&\
	(off) < (pop)->heap_offset + (pop)->heap_size)
#define	OBJ_OFF_FROM_LANES(pop, off)\
	((off) >= (pop)->lanes_offset &&\
	(off) < (pop)->lanes_offset +\
	(pop)->nlanes * sizeof (struct lane_layout))
#define	OBJ_OFF_FROM_OBJ_STORE(pop, off)\
	((off) >= (pop)->obj_store_offset &&\
	(off) < (pop)->obj_store_offset + (pop)->obj_store_size)

#define	OBJ_OFF_IS_VALID(pop, off)\
	(OBJ_OFF_FROM_HEAP(pop, off) ||\
	OBJ_OFF_FROM_OBJ_STORE(pop, off))

#define	OBJ_PTR_IS_VALID(pop, ptr)\
	OBJ_OFF_IS_VALID(pop, OBJ_PTR_TO_OFF(pop, ptr))

#define	OBJ_OID_IS_VALID(pop, oid) (\
	{\
		PMEMoid o = (oid);\
		OBJ_OID_IS_NULL(o) ||\
		(o.pool_uuid_lo == (pop)->uuid_lo &&\
		o.off >= (pop)->heap_offset &&\
		o.off < (pop)->heap_offset + (pop)->heap_size);\
	})

#define	OOB_HEADER_FROM_OID(pop, oid)\
	((struct oob_header *)((uintptr_t)(pop) + (oid).off - OBJ_OOB_SIZE))

#define	OOB_HEADER_FROM_PTR(ptr)\
	((struct oob_header *)((uintptr_t)(ptr) - OBJ_OOB_SIZE))

#define	OOB_OFFSET_OF(oid, field)\
	((oid).off - OBJ_OOB_SIZE + offsetof(struct oob_header, field))

#define	OBJ_STORE_ITEM_PADDING\
	(_POBJ_CL_ALIGNMENT - (sizeof (struct list_head) % _POBJ_CL_ALIGNMENT))

typedef void (*persist_local_fn)(void *, size_t);
typedef void (*flush_local_fn)(void *, size_t);
typedef void (*drain_local_fn)(void);
typedef void *(*memcpy_local_fn)(void *dest, const void *src, size_t len);
typedef void *(*memset_local_fn)(void *dest, int c, size_t len);

typedef void (*persist_fn)(PMEMobjpool *pop, void *, size_t);
typedef void (*flush_fn)(PMEMobjpool *pop, void *, size_t);
typedef void (*drain_fn)(PMEMobjpool *pop);
typedef void *(*memcpy_fn)(PMEMobjpool *pop, void *dest, const void *src,
					size_t len);
typedef void *(*memset_fn)(PMEMobjpool *pop, void *dest, int c, size_t len);

typedef uint16_t type_num_t;
extern unsigned long Pagesize;

struct pmemobjpool {
	struct pool_hdr hdr;	/* memory pool header */

	/* persistent part of PMEMOBJ pool descriptor (2kB) */
	char layout[PMEMOBJ_MAX_LAYOUT];
	uint64_t lanes_offset;
	uint64_t nlanes;
	uint64_t obj_store_offset;
	uint64_t obj_store_size;
	uint64_t heap_offset;
	uint64_t heap_size;
	unsigned char unused[OBJ_DSC_P_UNUSED]; /* must be zero */
	uint64_t checksum;	/* checksum of above fields */

	/* unique runID for this program run - persistent but not checksummed */
	uint64_t run_id;

	/* some run-time state, allocated out of memory pool... */
	void *addr;		/* mapped region */
	size_t size;		/* size of mapped region */
	int is_pmem;		/* true if pool is PMEM */
	int rdonly;		/* true if pool is opened read-only */
	struct pmalloc_heap *heap; /* allocator heap */
	struct lane *lanes;
	struct object_store *store; /* object store */
	uint64_t uuid_lo;

	struct pmemobjpool *replica;	/* next replica */

	/* per-replica functions: pmem or non-pmem */
	persist_local_fn persist_local;	/* persist function */
	flush_local_fn flush_local;	/* flush function */
	drain_local_fn drain_local;	/* drain function */
	memcpy_local_fn memcpy_persist_local; /* persistent memcpy function */
	memset_local_fn memset_persist_local; /* persistent memset function */

	/* for 'master' replica: with or without data replication */
	persist_fn persist;	/* persist function */
	flush_fn flush;		/* flush function */
	drain_fn drain;		/* drain function */
	memcpy_fn memcpy_persist; /* persistent memcpy function */
	memset_fn memset_persist; /* persistent memset function */

	PMEMmutex rootlock;	/* root object lock */
	int is_master_replica;
	char unused2[1824];
};

struct oob_header_data {
	uint16_t internal_type;
	type_num_t user_type;
	uint8_t padding[4];
};

/*
 * Out-Of-Band Header - it is padded to 48B to fit one cache line (64B)
 * together with allocator's header (of size 16B) located just before it.
 */
struct oob_header {
	struct list_entry oob;
	size_t size;		/* used only in root object */
	struct oob_header_data data;
};

enum internal_type {
	TYPE_NONE,
	TYPE_ALLOCATED,

	MAX_INTERNAL_TYPE
};

/* single object store item */
struct object_store_item {
	struct list_head head;
	uint8_t padding[OBJ_STORE_ITEM_PADDING];
};

struct object_store {
	struct object_store_item root;
	struct object_store_item bytype[PMEMOBJ_NUM_OID_TYPES];
};

enum tx_state {
	TX_STATE_NONE = 0,
	TX_STATE_COMMITTED = 1,
};

struct tx_range {
	uint64_t offset;
	uint64_t size;
	uint8_t data[];
};

struct tx_range_cache {
	struct { /* compatible with struct tx_range */
		uint64_t offset;
		uint64_t size;
		uint8_t data[MAX_CACHED_RANGE_SIZE];
	} range[MAX_CACHED_RANGES];
};

struct lane_tx_layout {
	uint64_t state;
	struct list_head undo_alloc;
	struct list_head undo_free;
	struct list_head undo_set;
	struct list_head undo_set_cache;
};

static inline PMEMoid
oob_list_next(PMEMobjpool *pop, struct list_head *head, PMEMoid oid)
{
	struct oob_header *oobh = OOB_HEADER_FROM_OID(pop, oid);
	if (head->pe_first.off == oobh->oob.pe_next.off)
		return OID_NULL;

	return oobh->oob.pe_next;
}

static inline PMEMoid
oob_list_last(PMEMobjpool *pop, struct list_head *head)
{
	if (OBJ_OID_IS_NULL(head->pe_first))
		return OID_NULL;

	struct oob_header *oobh = OOB_HEADER_FROM_OID(pop, head->pe_first);
	return oobh->oob.pe_prev;
}

/*
 * pmemobj_get_uuid_lo -- (internal) evaluates XOR sum of least significant
 * 8 bytes with most significant 8 bytes.
 */
static inline uint64_t
pmemobj_get_uuid_lo(PMEMobjpool *pop)
{
	uint64_t uuid_lo = 0;

	for (int i = 0; i < 8; i++) {
		uuid_lo = (uuid_lo << 8) |
			(pop->hdr.poolset_uuid[i] ^
				pop->hdr.poolset_uuid[8 + i]);
	}

	return uuid_lo;
}

void obj_init(void);
void obj_fini(void);
