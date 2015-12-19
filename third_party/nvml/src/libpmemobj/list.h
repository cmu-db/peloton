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
 * list.h -- internal definitions for persistent atomic lists module
 */

#define	REDO_NUM_ENTRIES \
((LANE_SECTION_LEN - 2 * sizeof (uint64_t)) / sizeof (struct redo_log))

/*
 * lane_list_section -- structure of list section in lane
 *
 * obj_offset - offset to object which should be freed
 * obj_size   - size of object which was reallocated
 * redo       - redo log
 */
struct lane_list_section {
	uint64_t obj_offset;
	uint64_t obj_size;
	struct redo_log redo[REDO_NUM_ENTRIES];
};

struct list_entry {
	PMEMoid pe_next;
	PMEMoid pe_prev;
};

struct list_head {
	PMEMoid pe_first;
	PMEMmutex lock;
};

int list_insert_new(PMEMobjpool *pop, struct list_head *oob_head,
	size_t pe_offset, struct list_head *head, PMEMoid dest, int before,
	size_t size, void (*constructor)(PMEMobjpool *pop, void *ptr,
	void *arg), void *arg, PMEMoid *oidp);

int list_realloc(PMEMobjpool *pop, struct list_head *oob_head,
	size_t pe_offset, struct list_head *head,
	size_t size, void (*constructor)(PMEMobjpool *pop, void *ptr,
	void *arg), void *arg, uint64_t field_offset, uint64_t field_value,
	PMEMoid *oidp);

int list_realloc_move(PMEMobjpool *pop, struct list_head *oob_head_old,
	struct list_head *oob_head_new, size_t pe_offset,
	struct list_head *head, size_t size,
	void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg), void *arg,
	uint64_t field_offset, uint64_t field_value, PMEMoid *oidp);

int list_insert(PMEMobjpool *pop,
	size_t pe_offset, struct list_head *head, PMEMoid dest, int before,
	PMEMoid oid);

int list_remove_free(PMEMobjpool *pop, struct list_head *oob_head,
	size_t pe_offset, struct list_head *head,
	PMEMoid *oidp);

int list_remove(PMEMobjpool *pop,
	size_t pe_offset, struct list_head *head,
	PMEMoid oid);

int list_move(PMEMobjpool *pop,
	size_t pe_offset_old, struct list_head *head_old,
	size_t pe_offset_new, struct list_head *head_new,
	PMEMoid dest, int before, PMEMoid oid);

int list_move_oob(PMEMobjpool *pop,
	struct list_head *head_old, struct list_head *head_new,
	PMEMoid oid);
