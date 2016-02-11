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
 * obj_store_mocks.c -- mocks for unit tests for root object and object store,
 */

#include <inttypes.h>
#include <sys/param.h>

#include "unittest.h"
#include "util.h"
#include "lane.h"
#include "redo.h"
#include "list.h"
#include "obj.h"
#include "pmalloc.h"
#include "heap_layout.h"

struct heap_header_mock {
	uint64_t offset;	/* persistent */
	uint64_t size;		/* persistent */
	uint64_t pop;		/* volatile   */
};

/*
 * heap_init -- heap_init mock
 */
FUNC_MOCK(heap_init, int, PMEMobjpool *pop)
FUNC_MOCK_RUN_DEFAULT {
	struct heap_header_mock *hheader =
		(struct heap_header_mock *)((uint64_t)pop + pop->heap_offset);
	hheader->offset = pop->heap_offset + sizeof (struct heap_header_mock);
	hheader->size   = pop->heap_size   - sizeof (struct heap_header_mock);
	pmem_msync(hheader, sizeof (*hheader));
	return 0;
}
FUNC_MOCK_END

/*
 * heap_boot -- heap_boot mock
 */
FUNC_MOCK(heap_boot, int, PMEMobjpool *pop)
FUNC_MOCK_RUN_DEFAULT {
	struct heap_header_mock *hheader =
		(struct heap_header_mock *)((uint64_t)pop + pop->heap_offset);
	hheader->pop = (uint64_t)pop;
	pop->heap = (struct pmalloc_heap *)hheader;
	return 0;
}
FUNC_MOCK_END

/*
 * heap_cleanup -- heap_cleanup mock
 */
FUNC_MOCK(heap_cleanup, int, PMEMobjpool *pop)
FUNC_MOCK_RUN_DEFAULT {
	return ENOSYS;
}
FUNC_MOCK_END

/*
 * pmalloc -- pmalloc mock
 *
 * Allocates the memory using linear allocator.
 */
FUNC_MOCK(pmalloc, int, PMEMobjpool *pop, uint64_t *off, size_t size,
		uint64_t data_off)
FUNC_MOCK_RUN_DEFAULT {
	struct heap_header_mock *hheader = (struct heap_header_mock *)pop->heap;
	PMEMobjpool *pop = (PMEMobjpool *)hheader->pop;
	if (size < hheader->size) {
		struct allocation_header *alloc =
				(void *)(hheader->pop + hheader->offset);
		alloc->size = size;
		alloc->chunk_id = alloc->zone_id = 0;
		pop->persist(pop, alloc, sizeof (*alloc));
		*off = hheader->offset + sizeof (*alloc);
		pop->persist(pop, off, sizeof (uint64_t));
		hheader->offset += roundup(size, sizeof (uint64_t));
		hheader->offset += sizeof (*alloc);
		hheader->size -= size + sizeof (*alloc);
		pop->persist(pop, hheader, sizeof (*hheader));
		return 0;
	} else
		return ENOMEM;
}
FUNC_MOCK_END

/*
 * pmalloc_construct -- pmalloc_construct mock
 */
FUNC_MOCK(pmalloc_construct, int, PMEMobjpool *pop, uint64_t *off,
	size_t size, void (*constructor)(PMEMobjpool *pop, void *ptr,
	void *arg), void *arg, uint64_t data_off)
FUNC_MOCK_RUN_DEFAULT {
	struct heap_header_mock *hheader = (struct heap_header_mock *)pop->heap;
	if (pmalloc(pop, off, size, data_off))
		return ENOMEM;
	else {
		(*constructor)(pop, (void *)(hheader->pop + *off + data_off),
			arg);
		return 0;
	}
}
FUNC_MOCK_END

/*
 * prealloc -- prealloc mock
 */
FUNC_MOCK(prealloc, int, PMEMobjpool *pop, uint64_t *off, size_t size,
		uint64_t data_off)
FUNC_MOCK_RUN_DEFAULT {
	return ENOSYS;
}
FUNC_MOCK_END

/*
 * prealloc_construct -- prealloc_construct mock
 */
FUNC_MOCK(prealloc_construct, int, PMEMobjpool *pop, uint64_t *off,
	size_t size, void (*constructor)(PMEMobjpool *pop, void *ptr,
	void *arg), void *arg, uint64_t data_off)
FUNC_MOCK_RUN_DEFAULT {
	struct heap_header_mock *hheader = (struct heap_header_mock *)pop->heap;
	if (prealloc(pop, off, size, data_off))
		return ENOMEM;
	else {
		(*constructor)(pop, (void *)(hheader->pop + *off + data_off),
			arg);
		return 0;
	}
}
FUNC_MOCK_END

/*
 * pmalloc_usable_size -- pmalloc_usable_size mock
 */
FUNC_MOCK(pmalloc_usable_size, size_t, PMEMobjpool *pop, uint64_t off)
FUNC_MOCK_RUN_DEFAULT {
	struct heap_header_mock *hheader = (struct heap_header_mock *)pop->heap;
	struct allocation_header *alloc =
			(void *)(hheader->pop + off - sizeof (*alloc));
	return alloc->size;
}
FUNC_MOCK_END

/*
 * pfree -- pfree mock
 */
FUNC_MOCK(pfree, int, PMEMobjpool *pop, uint64_t *off, uint64_t data_off)
FUNC_MOCK_RUN_DEFAULT {
	struct heap_header_mock *hheader = (struct heap_header_mock *)pop->heap;
	PMEMobjpool *pop = (PMEMobjpool *)hheader->pop;
	struct allocation_header *alloc =
			(void *)(hheader->pop + *off - sizeof (*alloc));
	*off = 0;
	pop->persist(pop, off, sizeof (uint64_t));
	alloc->size = 0;
	pop->persist(pop, &alloc->size, sizeof (alloc->size));
	return 0;
}
FUNC_MOCK_END

FUNC_MOCK(heap_vg_open, void, PMEMobjpool *pop)
FUNC_MOCK_RUN_DEFAULT {}
FUNC_MOCK_END
