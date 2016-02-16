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
 * obj_lane.c -- unit test for lanes
 */
#ifndef	_GNU_SOURCE
#define	_GNU_SOURCE
#endif

#include <pthread.h>
#include <errno.h>

#include "unittest.h"
#include "libpmemobj.h"
#include "util.h"
#include "lane.h"
#include "redo.h"
#include "heap_layout.h"
#include "list.h"
#include "obj.h"

#define	MAX_MOCK_LANES 5
#define	MOCK_RUNTIME (void *)(0xABC)
#define	MOCK_RUNTIME_2 (void *)(0xBCD)

static void *base_ptr;
#define	RPTR(p) (void *)((char *)p - (char *)base_ptr)

struct mock_pop {
	PMEMobjpool p;
	struct lane_layout l[MAX_MOCK_LANES];
};

static int construct_fail;

static int
lane_noop_construct(PMEMobjpool *pop, struct lane_section *section)
{
	OUT("lane_noop_construct");
	if (construct_fail)
		return EINVAL;

	section->runtime = MOCK_RUNTIME;

	return 0;
}

static int
lane_noop_destruct(PMEMobjpool *pop, struct lane_section *section)
{
	OUT("lane_noop_destruct");
	return 0;
}

static int recovery_check_fail;

static int
lane_noop_recovery(PMEMobjpool *pop,
	struct lane_section_layout *section)
{
	OUT("lane_noop_recovery %p", RPTR(section));
	if (recovery_check_fail)
		return EINVAL;

	return 0;
}

static int
lane_noop_check(PMEMobjpool *pop,
	struct lane_section_layout *section)
{
	OUT("lane_noop_check %p", RPTR(section));
	if (recovery_check_fail)
		return EINVAL;

	return 0;
}

static int
lane_noop_boot(PMEMobjpool *pop)
{
	OUT("lane_noop_init");

	return 0;
}

struct section_operations noop_ops = {
	.construct = lane_noop_construct,
	.destruct = lane_noop_destruct,
	.recover = lane_noop_recovery,
	.check = lane_noop_check,
	.boot = lane_noop_boot
};

SECTION_PARM(LANE_SECTION_ALLOCATOR, &noop_ops);
SECTION_PARM(LANE_SECTION_LIST, &noop_ops);
SECTION_PARM(LANE_SECTION_TRANSACTION, &noop_ops);

static void
test_lane_boot_cleanup_ok()
{
	struct mock_pop pop = {
		.p = {
			.nlanes = MAX_MOCK_LANES,
			.lanes = NULL
		}
	};
	base_ptr = &pop.p;

	pop.p.lanes_offset = (uint64_t)&pop.l - (uint64_t)&pop.p;
	ASSERTeq(lane_boot(&pop.p), 0);
	ASSERTne(pop.p.lanes, NULL);
	for (int i = 0; i < MAX_MOCK_LANES; ++i) {
		for (int j = 0; j < MAX_LANE_SECTION; ++j) {
			ASSERTeq(pop.p.lanes[i].sections[j].layout,
				&pop.l[i].sections[j]);
			ASSERTeq(pop.p.lanes[i].sections[j].runtime,
				MOCK_RUNTIME);
		}
	}

	ASSERTeq(lane_cleanup(&pop.p), 0);
	ASSERTeq(pop.p.lanes, NULL);
}

static void
test_lane_boot_fail()
{
	struct mock_pop pop = {
		.p = {
			.nlanes = MAX_MOCK_LANES,
			.lanes = NULL
		}
	};
	base_ptr = &pop.p;
	pop.p.lanes_offset = (uint64_t)&pop.l - (uint64_t)&pop.p;

	construct_fail = 1;

	ASSERTne(lane_boot(&pop.p), 0);

	construct_fail = 0;

	ASSERTeq(pop.p.lanes, NULL);
}

static void
test_lane_recovery_check_ok()
{
	struct mock_pop pop = {
		.p = {
			.nlanes = MAX_MOCK_LANES,
			.lanes = NULL
		}
	};
	base_ptr = &pop.p;
	pop.p.lanes_offset = (uint64_t)&pop.l - (uint64_t)&pop.p;

	ASSERTeq(lane_recover_and_section_boot(&pop.p), 0);
	ASSERTeq(lane_check(&pop.p), 0);
}

static void
test_lane_recovery_check_fail()
{
	struct mock_pop pop = {
		.p = {
			.nlanes = MAX_MOCK_LANES,
			.lanes = NULL
		}
	};
	base_ptr = &pop.p;
	pop.p.lanes_offset = (uint64_t)&pop.l - (uint64_t)&pop.p;

	recovery_check_fail = 1;

	ASSERTne(lane_recover_and_section_boot(&pop.p), 0);

	ASSERTne(lane_check(&pop.p), 0);
}

static void
test_lane_hold_release()
{
	pthread_mutex_t lock = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

	struct lane mock_lane = {
		.lock = &lock,
		.sections = {
			[LANE_SECTION_ALLOCATOR] = {
				.runtime = MOCK_RUNTIME
			},
			[LANE_SECTION_LIST] = {
				.runtime = MOCK_RUNTIME_2
			}
		}
	};

	struct mock_pop pop = {
		.p = {
			.nlanes = 1,
			.lanes = &mock_lane
		}
	};
	pop.p.lanes_offset = (uint64_t)&pop.l - (uint64_t)&pop.p;
	base_ptr = &pop.p;

	struct lane_section *sec;
	ASSERTeq(lane_hold(&pop.p, &sec, LANE_SECTION_ALLOCATOR), 0);
	ASSERTeq(sec->runtime, MOCK_RUNTIME);
	ASSERTeq(lane_hold(&pop.p, &sec, LANE_SECTION_LIST), 0);
	ASSERTeq(sec->runtime, MOCK_RUNTIME_2);

	ASSERTeq(lane_release(&pop.p), 0);
	ASSERTeq(lane_release(&pop.p), 0);
	ASSERTne(lane_release(&pop.p), 0); /* only two sections were held */
}

static void
test_lane_sizes(void)
{
	ASSERT(sizeof (struct lane_tx_layout) <= LANE_SECTION_LEN);
	ASSERT(sizeof (struct allocator_lane_section) <= LANE_SECTION_LEN);
	ASSERT(sizeof (struct lane_list_section) <= LANE_SECTION_LEN);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_lane");

	test_lane_boot_cleanup_ok();
	test_lane_boot_fail();
	test_lane_recovery_check_ok();
	test_lane_recovery_check_fail();
	test_lane_hold_release();
	test_lane_sizes();

	DONE(NULL);
}
