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
 * lane.h -- internal definitions for lanes
 */
#define	LANE_SECTION_LEN 1024

enum lane_section_type {
	LANE_SECTION_ALLOCATOR,
	LANE_SECTION_LIST,
	LANE_SECTION_TRANSACTION,

	MAX_LANE_SECTION
};

struct lane_section_layout {
	unsigned char data[LANE_SECTION_LEN];
};

struct lane_section {
	/* persistent */
	struct lane_section_layout *layout;

	void *runtime;
};

struct lane_layout {
	struct lane_section_layout sections[MAX_LANE_SECTION];
};

struct lane {
	/* volatile state */
	pthread_mutex_t *lock;
	struct lane_section sections[MAX_LANE_SECTION];
};

typedef int (*section_layout_op)(PMEMobjpool *pop,
	struct lane_section_layout *layout);
typedef int (*section_op)(PMEMobjpool *pop, struct lane_section *section);
typedef int (*section_global_op)(PMEMobjpool *pop);

struct section_operations {
	section_op construct;
	section_op destruct;
	section_layout_op check;
	section_layout_op recover;
	section_global_op boot;
};

extern struct section_operations *Section_ops[MAX_LANE_SECTION];
extern __thread unsigned Lane_idx;

int lane_boot(PMEMobjpool *pop);
int lane_cleanup(PMEMobjpool *pop);
int lane_recover_and_section_boot(PMEMobjpool *pop);
int lane_check(PMEMobjpool *pop);

int lane_hold(PMEMobjpool *pop, struct lane_section **section,
	enum lane_section_type type);
int lane_release(PMEMobjpool *pop);

#define	SECTION_PARM(n, ops)\
__attribute__((constructor)) static void _section_parm_##n(void)\
{ Section_ops[n] = ops; }
