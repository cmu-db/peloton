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
 * obj_persist_count.c -- counting number of persists
 */
#include "unittest.h"
#include "pmalloc.h"

static struct {
	int n_persist;
	int n_msync;
	int n_flush;
	int n_drain;
} ops_counter;

FUNC_MOCK(pmem_persist, void, void *addr, size_t len)
	FUNC_MOCK_RUN_DEFAULT {
		ops_counter.n_persist++;
		_FUNC_REAL(pmem_persist)(addr, len);
	}
FUNC_MOCK_END

FUNC_MOCK(pmem_msync, int, void *addr, size_t len)
	FUNC_MOCK_RUN_DEFAULT {
		ops_counter.n_msync++;
		return _FUNC_REAL(pmem_msync)(addr, len);
	}
FUNC_MOCK_END

FUNC_MOCK(pmem_flush, void, void *addr, size_t len)
	FUNC_MOCK_RUN_DEFAULT {
		ops_counter.n_flush++;
		_FUNC_REAL(pmem_flush)(addr, len);
	}
FUNC_MOCK_END

FUNC_MOCK(pmem_drain, void, void)
	FUNC_MOCK_RUN_DEFAULT {
		ops_counter.n_drain++;
		_FUNC_REAL(pmem_drain)();
	}
FUNC_MOCK_END

/*
 * reset_counters -- zero all counters
 */
static void
reset_counters(void)
{
	memset(&ops_counter, 0, sizeof (ops_counter));
}

/*
 * print_reset_counters -- print and then zero all counters
 */
static void
print_reset_counters(const char *task)
{
	OUT("%d\t;%d\t;%d\t;%d\t;%s",
		ops_counter.n_persist, ops_counter.n_msync,
		ops_counter.n_flush, ops_counter.n_drain, task);

	reset_counters();
}

struct foo {
	int val;
	uint64_t dest;

	PMEMoid bar;
};

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_persist_count");

	if (argc != 2)
		FATAL("usage: %s file-name", argv[0]);

	const char *path = argv[1];

	PMEMobjpool *pop;
	if ((pop = pmemobj_create(path, "persist_count",
			PMEMOBJ_MIN_POOL, S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!pmemobj_create: %s", path);

	OUT("persist\t;msync\t;flush\t;drain\t;task");

	print_reset_counters("pool_create");

	/* allocate one structure to create a run */
	pmemobj_alloc(pop, NULL, sizeof (struct foo), 0, NULL, NULL);
	reset_counters();

	PMEMoid root = pmemobj_root(pop, sizeof (struct foo));
	ASSERT(!OID_IS_NULL(root));
	print_reset_counters("root_alloc");

	PMEMoid oid;
	int ret = pmemobj_alloc(pop, &oid, sizeof (struct foo), 0, NULL, NULL);
	ASSERTeq(ret, 0);
	print_reset_counters("atomic_alloc");

	pmemobj_free(&oid);
	print_reset_counters("atomic_free");

	struct foo *f = pmemobj_direct(root);

	TX_BEGIN(pop) {
		f->bar = pmemobj_tx_alloc(sizeof (struct foo), 0);
		ASSERT(!OID_IS_NULL(f->bar));
	} TX_END
	print_reset_counters("tx_alloc");

	TX_BEGIN(pop) {
		pmemobj_tx_free(f->bar);
	} TX_END
	print_reset_counters("tx_free");

	TX_BEGIN(pop) {
		pmemobj_tx_add_range_direct(&f->val, sizeof (f->val));
	} TX_END
	print_reset_counters("tx_add");

	pmalloc(pop, &f->dest, sizeof (f->val), 0);
	print_reset_counters("pmalloc");

	pfree(pop, &f->dest, 0);
	print_reset_counters("pfree");

	pmemobj_close(pop);

	DONE(NULL);
}
