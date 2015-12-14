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
 * list.c -- implementation of persistent atomic lists module
 */
#include <errno.h>

#include "libpmemobj.h"
#include "lane.h"
#include "redo.h"
#include "list.h"
#include "pmalloc.h"
#include "util.h"
#include "obj.h"
#include "out.h"
#include "valgrind_internal.h"

#define	PREV_OFF (offsetof(struct list_entry, pe_prev) + offsetof(PMEMoid, off))
#define	NEXT_OFF (offsetof(struct list_entry, pe_next) + offsetof(PMEMoid, off))
#define	OOB_ENTRY_OFF (offsetof(struct oob_header, oob))
#define	OOB_ENTRY_OFF_REV \
((ssize_t)offsetof(struct oob_header, oob) - (ssize_t)OBJ_OOB_SIZE)

/*
 * list_args_common -- common arguments for operations on list
 *
 * pe_offset    - offset to list entry relative to user data
 * obj_doffset  - offset to element's data relative to pmemobj pool
 * entry_ptr    - list entry structure of element
 */
struct list_args_common {
	ssize_t pe_offset;
	uint64_t obj_doffset;
	struct list_entry *entry_ptr;
};

/*
 * list_args_insert -- arguments for inserting element to list
 *
 * head           - list head
 * dest           - destination element OID
 * dest_entry_ptr - list entry of destination element
 * before         - insert before or after destination element
 */
struct list_args_insert {
	struct list_head *head;
	PMEMoid dest;
	struct list_entry *dest_entry_ptr;
	int before;
};

/*
 * list_args_reinsert -- arguments for reinserting element on list
 *
 * head         - list head
 * entry_ptr    - list entry of old element
 * obj_doffset  - offset to element's data relative to pmemobj pool
 */
struct list_args_reinsert {
	struct list_head *head;
	struct list_entry *entry_ptr;
	uint64_t obj_doffset;
};

/*
 * list_args_remove -- arguments for removing element from list
 *
 * pe_offset    - offset to list entry relative to user data
 * obj_doffset  - offset to element's data relative to pmemobj pool
 * head         - list head
 * entry_ptr    - list entry structure of element
 */
struct list_args_remove {
	ssize_t pe_offset;
	uint64_t obj_doffset;
	struct list_head *head;
	struct list_entry *entry_ptr;
};

/*
 * list_mutexes_lock -- (internal) grab one or two locks in ascending
 * address order
 */
static inline int
list_mutexes_lock(PMEMobjpool *pop,
	struct list_head *head1, struct list_head *head2)
{
	ASSERTne(head1, NULL);

	if (!head2)
		return pmemobj_mutex_lock(pop, &head1->lock);

	PMEMmutex *lock1;
	PMEMmutex *lock2;
	if ((uintptr_t)&head1->lock < (uintptr_t)&head2->lock) {
		lock1 = &head1->lock;
		lock2 = &head2->lock;
	} else {
		lock1 = &head2->lock;
		lock2 = &head1->lock;
	}

	int ret;
	if ((ret = pmemobj_mutex_lock(pop, lock1)))
		goto err;
	if ((ret = pmemobj_mutex_lock(pop, lock2)))
		goto err_unlock;

	return 0;
err_unlock:
	pmemobj_mutex_unlock(pop, lock1);
err:
	return ret;
}

/*
 * list_mutexes_unlock -- (internal) release one or two locks
 */
static inline int
list_mutexes_unlock(PMEMobjpool *pop,
	struct list_head *head1, struct list_head *head2)
{
	ASSERTne(head1, NULL);

	if (!head2)
		return pmemobj_mutex_unlock(pop, &head1->lock);

	int ret1 = pmemobj_mutex_unlock(pop, &head1->lock);
	int ret2 = pmemobj_mutex_unlock(pop, &head2->lock);

	return ret2 ? ret2 : ret1;
}

/*
 * list_get_dest -- (internal) return destination object ID
 *
 * If the input dest is not OID_NULL returns dest.
 * If the input dest is OID_NULL and before is set returns first element.
 * If the input dest is OID_NULL and before is no set returns last element.
 */
static inline PMEMoid
list_get_dest(PMEMobjpool *pop, struct list_head *head, PMEMoid dest,
		uint64_t pe_offset, int before)
{
	ASSERT(before == POBJ_LIST_DEST_HEAD || before == POBJ_LIST_DEST_TAIL);

	if (dest.off)
		return dest;

	if (head->pe_first.off == 0 || before == POBJ_LIST_DEST_HEAD)
		return head->pe_first;

	struct list_entry *first_ptr = (struct list_entry *)OBJ_OFF_TO_PTR(pop,
			head->pe_first.off + pe_offset);

	return first_ptr->pe_prev;
}

/*
 * list_set_oid_redo_log -- (internal) set PMEMoid value using redo log
 */
static size_t
list_set_oid_redo_log(PMEMobjpool *pop,
	struct redo_log *redo, size_t redo_index,
	PMEMoid *oidp, uint64_t obj_doffset, int oidp_inited)
{
	ASSERT(OBJ_PTR_IS_VALID(pop, oidp));

	if (!oidp_inited || oidp->pool_uuid_lo != pop->uuid_lo) {
		if (oidp_inited)
			ASSERTeq(oidp->pool_uuid_lo, 0);
		uint64_t oid_uuid_off = OBJ_PTR_TO_OFF(pop,
				&oidp->pool_uuid_lo);
		redo_log_store(pop, redo, redo_index, oid_uuid_off,
				pop->uuid_lo);
		redo_index += 1;
	}

	uint64_t oid_off_off = OBJ_PTR_TO_OFF(pop, &oidp->off);
	redo_log_store(pop, redo, redo_index, oid_off_off, obj_doffset);

	return redo_index + 1;
}


/*
 * list_update_head -- (internal) update pe_first entry in list head
 */
static size_t
list_update_head(PMEMobjpool *pop,
	struct redo_log *redo, size_t redo_index,
	struct list_head *head, uint64_t first_offset)
{
	LOG(15, NULL);

	uint64_t pe_first_off_off = OBJ_PTR_TO_OFF(pop, &head->pe_first.off);

	redo_log_store(pop, redo, redo_index + 0,
			pe_first_off_off, first_offset);

	if (head->pe_first.pool_uuid_lo == 0) {
		uint64_t pe_first_uuid_off = OBJ_PTR_TO_OFF(pop,
				&head->pe_first.pool_uuid_lo);

		redo_log_store(pop, redo, redo_index + 1,
				pe_first_uuid_off, pop->uuid_lo);

		return redo_index + 2;
	} else {
		return redo_index + 1;
	}
}

/*
 * u64_add_offset -- (internal) add signed offset to unsigned integer and check
 * for overflows
 */
static void
u64_add_offset(uint64_t *value, ssize_t off)
{
	uint64_t prev = *value;
	if (off >= 0) {
		*value += (size_t)off;
		ASSERT(*value >= prev); /* detect overflow */
	} else {
		*value -= (size_t)-off;
		ASSERT(*value < prev);
	}
}

/*
 * list_replace_item -- (internal) replace not-first element on a single list
 */
static size_t
list_replace_item(PMEMobjpool *pop,
	struct redo_log *redo, size_t redo_index,
	struct list_args_reinsert *args,
	struct list_args_common *args_common,
	uint64_t *next_offset, uint64_t *prev_offset)
{
	LOG(15, NULL);

	/* return next and prev offsets values */
	*next_offset = args->entry_ptr->pe_next.off;
	*prev_offset = args->entry_ptr->pe_prev.off;

	/* modify prev->next and next->prev offsets */
	uint64_t prev_next_off = args->entry_ptr->pe_prev.off + NEXT_OFF;
	u64_add_offset(&prev_next_off, args_common->pe_offset);

	uint64_t next_prev_off = args->entry_ptr->pe_next.off + PREV_OFF;
	u64_add_offset(&next_prev_off, args_common->pe_offset);

	redo_log_store(pop, redo, redo_index + 0, prev_next_off,
			args_common->obj_doffset);
	redo_log_store(pop, redo, redo_index + 1, next_prev_off,
			args_common->obj_doffset);

	return redo_index + 2;
}

/*
 * list_replace_single -- (internal) replace an element on a single list
 */
static size_t
list_replace_single(PMEMobjpool *pop,
	struct redo_log *redo, size_t redo_index,
	struct list_args_reinsert *args, struct list_args_common *args_common,
	uint64_t *next_offset, uint64_t *prev_offset)
{
	LOG(15, NULL);

	if (args->entry_ptr->pe_next.off == args->obj_doffset) {
		ASSERTeq(args->entry_ptr->pe_prev.off, args->obj_doffset);
		ASSERTeq(args->head->pe_first.off, args->obj_doffset);

		/* replacing the only one element on list */
		*next_offset = args_common->obj_doffset;
		*prev_offset = args_common->obj_doffset;

		return list_update_head(pop, redo, redo_index,
				args->head, args_common->obj_doffset);
	} else {
		/* replacing nth element on list */
		redo_index = list_replace_item(pop, redo, redo_index,
				args, args_common, next_offset, prev_offset);

		/* modify head if reinserting element is the first one */
		if (args->head->pe_first.off == args->obj_doffset) {
			return list_update_head(pop, redo, redo_index,
					args->head, args_common->obj_doffset);
		} else {
			return redo_index;
		}
	}
}

/*
 * list_set_user_field -- (internal) set user's field using redo log
 */
static size_t
list_set_user_field(PMEMobjpool *pop,
	struct redo_log *redo, size_t redo_index,
	uint64_t field_offset, uint64_t field_value,
	uint64_t old_offset, uint64_t old_size,
	uint64_t new_offset)
{
	LOG(15, NULL);
	if (field_offset >= old_offset &&
		field_offset < old_offset + old_size) {
		ASSERT(field_offset + sizeof (uint64_t)
				<= old_offset + old_size);
		/*
		 * The user's field is inside the object so we don't need to
		 * use redo log - just normal store + persist.
		 */
		uint64_t new_field_offset = field_offset -
			old_offset + new_offset;

		uint64_t *field = OBJ_OFF_TO_PTR(pop, new_field_offset);
		/* temp add custom field change to valgrind transaction */
		VALGRIND_ADD_TO_TX(field, sizeof (*field));
		*field = field_value;
		VALGRIND_REMOVE_FROM_TX(field, sizeof (*field));
		pop->persist(pop, field, sizeof (*field));

		return redo_index;
	} else {
		/* field outside the object */
		redo_log_store(pop, redo, redo_index,
				field_offset, field_value);

		return redo_index + 1;
	}
}

/*
 * list_fill_entry_persist -- (internal) fill new entry using persist function
 *
 * Used for newly allocated objects.
 */
static void
list_fill_entry_persist(PMEMobjpool *pop, struct list_entry *entry_ptr,
		uint64_t next_offset, uint64_t prev_offset)
{
	LOG(15, NULL);

	VALGRIND_ADD_TO_TX(entry_ptr, sizeof (*entry_ptr));
	entry_ptr->pe_next.pool_uuid_lo = pop->uuid_lo;
	entry_ptr->pe_next.off = next_offset;

	entry_ptr->pe_prev.pool_uuid_lo = pop->uuid_lo;
	entry_ptr->pe_prev.off = prev_offset;
	VALGRIND_REMOVE_FROM_TX(entry_ptr, sizeof (*entry_ptr));

	pop->persist(pop, entry_ptr, sizeof (*entry_ptr));
}

/*
 * list_fill_entry_redo_log -- (internal) fill new entry using redo log
 *
 * Used to update entry in existing object.
 */
static size_t
list_fill_entry_redo_log(PMEMobjpool *pop,
	struct redo_log *redo, size_t redo_index,
	struct list_args_common *args,
	uint64_t next_offset, uint64_t prev_offset, int set_uuid)
{
	LOG(15, NULL);

	ASSERTne(args->entry_ptr, NULL);
	ASSERTne(args->obj_doffset, 0);

	if (set_uuid) {
		VALGRIND_ADD_TO_TX(&(args->entry_ptr->pe_next.pool_uuid_lo),
				sizeof (args->entry_ptr->pe_next.pool_uuid_lo));
		VALGRIND_ADD_TO_TX(&(args->entry_ptr->pe_prev.pool_uuid_lo),
				sizeof (args->entry_ptr->pe_prev.pool_uuid_lo));
		/* don't need to fill pool uuid using redo log */
		args->entry_ptr->pe_next.pool_uuid_lo = pop->uuid_lo;
		args->entry_ptr->pe_prev.pool_uuid_lo = pop->uuid_lo;
		VALGRIND_REMOVE_FROM_TX(
				&(args->entry_ptr->pe_next.pool_uuid_lo),
				sizeof (args->entry_ptr->pe_next.pool_uuid_lo));
		VALGRIND_REMOVE_FROM_TX(
				&(args->entry_ptr->pe_prev.pool_uuid_lo),
				sizeof (args->entry_ptr->pe_prev.pool_uuid_lo));
		pop->persist(pop, args->entry_ptr, sizeof (*args->entry_ptr));
	} else {
		ASSERTeq(args->entry_ptr->pe_next.pool_uuid_lo, pop->uuid_lo);
		ASSERTeq(args->entry_ptr->pe_prev.pool_uuid_lo, pop->uuid_lo);
	}

	/* set current->next and current->prev using redo log */
	uint64_t next_off_off = args->obj_doffset + NEXT_OFF;
	uint64_t prev_off_off = args->obj_doffset + PREV_OFF;
	u64_add_offset(&next_off_off, args->pe_offset);
	u64_add_offset(&prev_off_off, args->pe_offset);

	redo_log_store(pop, redo, redo_index + 0, next_off_off, next_offset);
	redo_log_store(pop, redo, redo_index + 1, prev_off_off, prev_offset);

	return redo_index + 2;
}

/*
 * list_remove_single -- (internal) remove element from single list
 */
static size_t
list_remove_single(PMEMobjpool *pop,
	struct redo_log *redo, size_t redo_index,
	struct list_args_remove *args)
{
	LOG(15, NULL);

	if (args->entry_ptr->pe_next.off == args->obj_doffset) {
		/* only one element on list */
		ASSERTeq(args->head->pe_first.off, args->obj_doffset);
		ASSERTeq(args->entry_ptr->pe_prev.off, args->obj_doffset);

		return list_update_head(pop, redo, redo_index, args->head, 0);
	} else {
		/* set next->prev = prev and prev->next = next */
		uint64_t next_off = args->entry_ptr->pe_next.off;
		uint64_t next_prev_off = next_off + PREV_OFF;
		u64_add_offset(&next_prev_off, args->pe_offset);
		uint64_t prev_off = args->entry_ptr->pe_prev.off;
		uint64_t prev_next_off = prev_off + NEXT_OFF;
		u64_add_offset(&prev_next_off, args->pe_offset);

		redo_log_store(pop, redo, redo_index + 0,
				next_prev_off, prev_off);
		redo_log_store(pop, redo, redo_index + 1,
				prev_next_off, next_off);
		redo_index += 2;

		if (args->head->pe_first.off == args->obj_doffset) {
			/* removing element is the first one */
			return list_update_head(pop, redo, redo_index,
					args->head, next_off);
		} else {
			return redo_index;
		}
	}
}

/*
 * list_insert_before -- (internal) insert element at offset before an element
 */
static size_t
list_insert_before(PMEMobjpool *pop,
	struct redo_log *redo, size_t redo_index,
	struct list_args_insert *args, struct list_args_common *args_common,
	uint64_t *next_offset, uint64_t *prev_offset)
{
	LOG(15, NULL);

	/* current->next = dest and current->prev = dest->prev */
	*next_offset = args->dest.off;
	*prev_offset = args->dest_entry_ptr->pe_prev.off;

	/* dest->prev = current and dest->prev->next = current */
	uint64_t dest_prev_off = args->dest.off + PREV_OFF;
	u64_add_offset(&dest_prev_off, args_common->pe_offset);
	uint64_t dest_prev_next_off = args->dest_entry_ptr->pe_prev.off +
					NEXT_OFF;
	u64_add_offset(&dest_prev_next_off, args_common->pe_offset);

	redo_log_store(pop, redo, redo_index + 0,
			dest_prev_off, args_common->obj_doffset);
	redo_log_store(pop, redo, redo_index + 1,
			dest_prev_next_off, args_common->obj_doffset);

	return redo_index + 2;
}

/*
 * list_insert_after -- (internal) insert element at offset after an element
 */
static size_t
list_insert_after(PMEMobjpool *pop,
	struct redo_log *redo, size_t redo_index,
	struct list_args_insert *args, struct list_args_common *args_common,
	uint64_t *next_offset, uint64_t *prev_offset)
{
	LOG(15, NULL);

	/* current->next = dest->next and current->prev = dest */
	*next_offset = args->dest_entry_ptr->pe_next.off;
	*prev_offset = args->dest.off;

	/* dest->next = current and dest->next->prev = current */
	uint64_t dest_next_off = args->dest.off + NEXT_OFF;
	u64_add_offset(&dest_next_off, args_common->pe_offset);
	uint64_t dest_next_prev_off = args->dest_entry_ptr->pe_next.off +
					PREV_OFF;
	u64_add_offset(&dest_next_prev_off, args_common->pe_offset);

	redo_log_store(pop, redo, redo_index + 0,
			dest_next_off, args_common->obj_doffset);
	redo_log_store(pop, redo, redo_index + 1,
			dest_next_prev_off, args_common->obj_doffset);

	return redo_index + 2;
}

/*
 * list_insert_user -- (internal) insert element at offset to a user list
 */
static size_t
list_insert_user(PMEMobjpool *pop,
	struct redo_log *redo, size_t redo_index,
	struct list_args_insert *args, struct list_args_common *args_common,
	uint64_t *next_offset, uint64_t *prev_offset)
{
	LOG(15, NULL);
	if (args->dest.off == 0) {
		/* inserting the first element on list */
		ASSERTeq(args->head->pe_first.off, 0);

		/* set loop on current element */
		*next_offset = args_common->obj_doffset;
		*prev_offset = args_common->obj_doffset;

		/* update head */
		redo_index = list_update_head(pop,
			redo, redo_index, args->head,
			args_common->obj_doffset);
	} else {
		if (args->before) {
			/* inserting before dest */
			redo_index = list_insert_before(pop,
				redo, redo_index, args, args_common,
				next_offset, prev_offset);

			if (args->dest.off == args->head->pe_first.off) {
				/* current element at first position */
				redo_index = list_update_head(pop,
					redo, redo_index, args->head,
					args_common->obj_doffset);
			}
		} else {
			/* inserting after dest */
			redo_index = list_insert_after(pop,
				redo, redo_index, args, args_common,
				next_offset, prev_offset);
		}
	}

	return redo_index;
}

/*
 * list_insert_oob -- (internal) inserting element to oob list
 *
 * This function inserts the element at the last position always.
 */
static size_t
list_insert_oob(PMEMobjpool *pop, struct redo_log *redo, size_t redo_index,
	struct list_head *oob_head, uint64_t obj_offset,
	uint64_t *next_offset, uint64_t *prev_offset)
{
	if (oob_head->pe_first.off == 0) {
		/* inserting the first element */

		/* set loop on current element */
		*next_offset = obj_offset;
		*prev_offset = obj_offset;

		/* update head */
		redo_index = list_update_head(pop, redo, redo_index,
				oob_head, obj_offset);

		return redo_index;
	} else {
		/* inserting at the last position */
		struct list_entry *first_ptr =
			(struct list_entry *)OBJ_OFF_TO_PTR(pop,
					oob_head->pe_first.off -
					OBJ_OOB_SIZE + OOB_ENTRY_OFF);

		/* current->next = first and current->prev = first->prev */
		*next_offset = oob_head->pe_first.off;
		*prev_offset = first_ptr->pe_prev.off;

		uint64_t first_prev_off = oob_head->pe_first.off -
				OBJ_OOB_SIZE + OOB_ENTRY_OFF + PREV_OFF;
		uint64_t first_prev_next_off = first_ptr->pe_prev.off -
				OBJ_OOB_SIZE + OOB_ENTRY_OFF + NEXT_OFF;

		redo_log_store(pop, redo, redo_index + 0,
				first_prev_off, obj_offset);
		redo_log_store(pop, redo, redo_index + 1,
				first_prev_next_off, obj_offset);

		return redo_index + 2;
	}
}

/*
 * list_realloc_replace -- (internal) realloc and replace element
 */
static size_t
list_realloc_replace(PMEMobjpool *pop,
	struct redo_log *redo, size_t redo_index,
	struct list_head *head, uint64_t pe_offset,
	size_t old_size,
	uint64_t obj_offset, uint64_t new_obj_offset,
	void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg), void *arg,
	uint64_t field_offset, uint64_t field_value)
{
	uint64_t obj_doffset = obj_offset + OBJ_OOB_SIZE;
	uint64_t new_obj_doffset = new_obj_offset + OBJ_OOB_SIZE;

	/* call the constructor manually */
	void *ptr = OBJ_OFF_TO_PTR(pop, new_obj_doffset);
	constructor(pop, ptr, arg);

	if (field_offset) {
		redo_index = list_set_user_field(pop,
				redo, redo_index,
				field_offset, field_value,
				obj_offset, old_size,
				new_obj_offset);
	}

	if (head) {
		struct list_entry *entry_ptr =
			(struct list_entry *)OBJ_OFF_TO_PTR(pop,
					obj_doffset + pe_offset);

		struct list_entry *new_entry_ptr =
			(struct list_entry *)OBJ_OFF_TO_PTR(pop,
					new_obj_doffset + pe_offset);

		struct list_args_reinsert args_reinsert = {
			.head = head,
			.entry_ptr = entry_ptr,
			.obj_doffset = obj_doffset,
		};

		ASSERT((ssize_t)pe_offset >= 0);
		struct list_args_common args_common = {
			.obj_doffset = new_obj_doffset,
			.entry_ptr = new_entry_ptr,
			.pe_offset = (ssize_t)pe_offset,
		};

		uint64_t next_offset;
		uint64_t prev_offset;

		/* replace element on user list */
		redo_index = list_replace_single(pop, redo, redo_index,
				&args_reinsert, &args_common,
				&next_offset, &prev_offset);

		/* fill next and prev offsets of new entry */
		list_fill_entry_persist(pop, new_entry_ptr,
				next_offset, prev_offset);
	}

	return redo_index;
}

/*
 * list_insert_new -- allocate and insert element to oob and user lists
 *
 * pop         - pmemobj pool handle
 * oob_head    - oob list head
 * pe_offset   - offset to list entry on user list relative to user data
 * head        - user list head
 * dest        - destination on user list
 * before      - insert before/after destination on user list
 * size        - size of allocation, will be increased by OBJ_OOB_SIZE
 * constructor - object's constructor
 * arg         - argument for object's constructor
 * oidp        - pointer to target object ID
 */
int
list_insert_new(PMEMobjpool *pop, struct list_head *oob_head,
	size_t pe_offset, struct list_head *head, PMEMoid dest, int before,
	size_t size, void (*constructor)(PMEMobjpool *pop, void *ptr,
	void *arg), void *arg,	PMEMoid *oidp)
{
	LOG(3, NULL);
	ASSERTne(oob_head, NULL);

	int ret;
	int out_ret;

	struct lane_section *lane_section;

	if ((ret = lane_hold(pop, &lane_section, LANE_SECTION_LIST))) {
		LOG(2, "lane_hold failed");
		return ret;
	}

	ASSERTne(lane_section, NULL);
	ASSERTne(lane_section->layout, NULL);

	/* increase allocation size by oob header size */
	size += OBJ_OOB_SIZE;
	struct lane_list_section *section =
		(struct lane_list_section *)lane_section->layout;
	struct redo_log *redo = section->redo;
	size_t redo_index = 0;
	uint64_t sec_off_off = OBJ_PTR_TO_OFF(pop, &section->obj_offset);

	if (constructor) {
		if ((ret = pmalloc_construct(pop,
				&section->obj_offset, size,
				constructor, arg, OBJ_OOB_SIZE))) {
			errno = ret;
			ERR("!pmalloc_construct");
			ret = -1;
			goto err_pmalloc;
		}
	} else {
		ret = pmalloc(pop, &section->obj_offset, size, OBJ_OOB_SIZE);
		if (ret) {
			errno = ret;
			ERR("!pmalloc");
			ret = -1;
			goto err_pmalloc;
		}
	}

	/*
	 * In case of oob list and user list grab the oob list lock
	 * first.
	 *
	 * XXX performance improvement: initialize oob locks at pool opening
	 */
	if ((ret = pmemobj_mutex_lock(pop, &oob_head->lock))) {
		LOG(2, "pmemobj_mutex_lock failed");
		goto err_oob_lock;
	}

	if (head) {
		if ((ret = pmemobj_mutex_lock(pop, &head->lock))) {
			LOG(2, "pmemobj_mutex_lock failed");
			goto err_lock;
		}
	}

	uint64_t obj_offset = section->obj_offset;
	uint64_t obj_doffset = obj_offset + OBJ_OOB_SIZE;

	struct list_entry *oob_entry_ptr =
		(struct list_entry *)OBJ_OFF_TO_PTR(pop,
			obj_offset + OOB_ENTRY_OFF);

	uint64_t oob_next_off;
	uint64_t oob_prev_off;

	/* insert element to oob list */
	redo_index = list_insert_oob(pop, redo, redo_index,
			oob_head, obj_doffset, &oob_next_off, &oob_prev_off);

	/* don't need to use redo log for filling new element */
	list_fill_entry_persist(pop, oob_entry_ptr, oob_next_off, oob_prev_off);

	if (head) {
		ASSERT((ssize_t)pe_offset >= 0);

		dest = list_get_dest(pop, head, dest, pe_offset, before);

		struct list_entry *entry_ptr =
			(struct list_entry *)OBJ_OFF_TO_PTR(pop,
					obj_doffset + pe_offset);

		struct list_entry *dest_entry_ptr =
			(struct list_entry *)OBJ_OFF_TO_PTR(pop,
					dest.off + pe_offset);

		struct list_args_insert args = {
			.dest = dest,
			.dest_entry_ptr = dest_entry_ptr,
			.head = head,
			.before = before,
		};

		struct list_args_common args_common = {
			.obj_doffset = obj_doffset,
			.entry_ptr = entry_ptr,
			.pe_offset = (ssize_t)pe_offset,
		};

		uint64_t next_offset;
		uint64_t prev_offset;

		/* insert element to user list */
		redo_index = list_insert_user(pop,
			redo, redo_index, &args, &args_common,
			&next_offset, &prev_offset);

		/* don't need to use redo log for filling new element */
		list_fill_entry_persist(pop, entry_ptr,
				next_offset, prev_offset);
	}

	if (oidp != NULL) {
		if (OBJ_PTR_IS_VALID(pop, oidp))
			redo_index = list_set_oid_redo_log(pop, redo,
					redo_index, oidp, obj_doffset, 0);
		else {
			oidp->off = obj_doffset;
			oidp->pool_uuid_lo = pop->uuid_lo;
		}
	}

	/* clear the obj_offset in lane section */
	redo_log_store_last(pop, redo, redo_index, sec_off_off, 0);

	redo_log_process(pop, redo, REDO_NUM_ENTRIES);

	ret = 0;

	if (head) {
		out_ret = pmemobj_mutex_unlock(pop, &head->lock);
		ASSERTeq(out_ret, 0);
		if (out_ret)
			LOG(2, "pmemobj_mutex_unlock failed");
	}
err_lock:
	out_ret = pmemobj_mutex_unlock(pop, &oob_head->lock);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "pmemobj_mutex_unlock failed");
err_pmalloc:
err_oob_lock:
	out_ret = lane_release(pop);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "lane_release failed");

	return ret;
}

/*
 * list_insert -- insert object to a single list
 *
 * pop          - pmemobj handle
 * pe_offset    - offset to list entry on user list relative to user data
 * head         - list head
 * dest         - destination object ID
 * before       - before/after destination
 * oid          - target object ID
 */
int
list_insert(PMEMobjpool *pop,
	size_t pe_offset, struct list_head *head,
	PMEMoid dest, int before,
	PMEMoid oid)
{
	LOG(3, NULL);
	ASSERTne(head, NULL);

	int ret;
	int out_ret;

	struct lane_section *lane_section;

	if ((ret = lane_hold(pop, &lane_section, LANE_SECTION_LIST))) {
		LOG(2, "lane_hold failed");
		return ret;
	}

	if ((ret = pmemobj_mutex_lock(pop, &head->lock))) {
		LOG(2, "pmemobj_mutex_lock failed");
		goto err;
	}

	ASSERTne(lane_section, NULL);
	ASSERTne(lane_section->layout, NULL);

	struct lane_list_section *section =
		(struct lane_list_section *)lane_section->layout;
	struct redo_log *redo = section->redo;
	size_t redo_index = 0;

	ASSERT((ssize_t)pe_offset >= 0);
	dest = list_get_dest(pop, head, dest, pe_offset, before);

	struct list_entry *entry_ptr =
		(struct list_entry *)OBJ_OFF_TO_PTR(pop,
				oid.off + pe_offset);

	struct list_entry *dest_entry_ptr =
		(struct list_entry *)OBJ_OFF_TO_PTR(pop,
				dest.off + pe_offset);

	struct list_args_insert args = {
		.dest = dest,
		.dest_entry_ptr = dest_entry_ptr,
		.head = head,
		.before = before,
	};

	struct list_args_common args_common = {
		.obj_doffset = oid.off,
		.entry_ptr = entry_ptr,
		.pe_offset = (ssize_t)pe_offset,
	};

	uint64_t next_offset;
	uint64_t prev_offset;

	/* insert element to user list */
	redo_index = list_insert_user(pop, redo, redo_index,
			&args, &args_common, &next_offset, &prev_offset);

	/* fill entry of existing element using redo log */
	redo_index = list_fill_entry_redo_log(pop, redo, redo_index,
			&args_common, next_offset, prev_offset, 1);

	redo_log_set_last(pop, redo, redo_index - 1);

	redo_log_process(pop, redo, REDO_NUM_ENTRIES);

	out_ret = pmemobj_mutex_unlock(pop, &head->lock);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "pmemobj_mutex_unlock failed");
err:
	out_ret = lane_release(pop);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "lane_release failed");

	return ret;
}

/*
 * list_remove_free -- remove from two lists and free an object
 *
 * pop         - pmemobj pool handle
 * oob_head    - oob list head
 * pe_offset   - offset to list entry on user list relative to user data
 * head        - user list head
 * oidp        - pointer to target object ID
 */
int
list_remove_free(PMEMobjpool *pop, struct list_head *oob_head,
	size_t pe_offset, struct list_head *head,
	PMEMoid *oidp)
{
	LOG(3, NULL);
	ASSERTne(oob_head, NULL);

	int ret;
	int out_ret;

	struct lane_section *lane_section;

	if ((ret = lane_hold(pop, &lane_section, LANE_SECTION_LIST))) {
		LOG(2, "lane_hold failed");
		return ret;
	}

	ASSERTne(lane_section, NULL);
	ASSERTne(lane_section->layout, NULL);

	/*
	 * In case of oob list and user list grab the oob list lock
	 * first.
	 *
	 * XXX performance improvement: initialize oob locks at pool opening
	 */
	if ((ret = pmemobj_mutex_lock(pop, &oob_head->lock))) {
		LOG(2, "pmemobj_mutex_lock failed");
		goto err_oob_lock;
	}

	if (head) {
		if ((ret = pmemobj_mutex_lock(pop, &head->lock))) {
			LOG(2, "pmemobj_mutex_lock failed");
			goto err_lock;
		}
	}

	struct lane_list_section *section =
		(struct lane_list_section *)lane_section->layout;
	uint64_t sec_off_off = OBJ_PTR_TO_OFF(pop, &section->obj_offset);
	struct redo_log *redo = section->redo;
	size_t redo_index = 0;

	uint64_t obj_doffset = oidp->off;
	uint64_t obj_offset = obj_doffset - OBJ_OOB_SIZE;

	struct list_entry *oob_entry_ptr =
		(struct list_entry *)OBJ_OFF_TO_PTR(pop,
				obj_offset + OOB_ENTRY_OFF);

	struct list_args_remove oob_args = {
		.pe_offset = OOB_ENTRY_OFF_REV,
		.head = oob_head,
		.entry_ptr = oob_entry_ptr,
		.obj_doffset = obj_doffset
	};

	/* remove from oob list */
	redo_index = list_remove_single(pop, redo, redo_index, &oob_args);

	if (head) {
		ASSERT((ssize_t)pe_offset >= 0);

		struct list_entry *entry_ptr =
			(struct list_entry *)OBJ_OFF_TO_PTR(pop,
					obj_doffset + pe_offset);

		struct list_args_remove args = {
			.pe_offset = (ssize_t)pe_offset,
			.head = head,
			.entry_ptr = entry_ptr,
			.obj_doffset = obj_doffset
		};

		/* remove from user list */
		redo_index = list_remove_single(pop, redo, redo_index, &args);
	}

	/* clear the oid */
	if (OBJ_PTR_IS_VALID(pop, oidp))
		redo_index = list_set_oid_redo_log(pop, redo, redo_index,
				oidp, 0, 1);
	else
		oidp->off = 0;

	redo_log_store_last(pop, redo, redo_index, sec_off_off, obj_offset);

	redo_log_process(pop, redo, REDO_NUM_ENTRIES);

	if (head) {
		out_ret = pmemobj_mutex_unlock(pop, &head->lock);
		ASSERTeq(out_ret, 0);
		if (out_ret)
			LOG(2, "pmemobj_mutex_unlock failed");
	}

	/*
	 * Don't need to fill next and prev offsets of removing element
	 * because the element is freed.
	 */
	if ((ret = pfree(pop, &section->obj_offset, OBJ_OOB_SIZE))) {
		errno = ret;
		ERR("!pfree");
		ret = -1;
	} else {
		ret = 0;
	}

err_lock:
	out_ret = pmemobj_mutex_unlock(pop, &oob_head->lock);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "pmemobj_mutex_unlock failed");
err_oob_lock:
	out_ret = lane_release(pop);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "lane_release failed");

	return ret;
}

/*
 * list_remove -- remove object from list
 *
 * pop          - pmemobj handle
 * pe_offset    - offset to list entry on user list relative to user data
 * head         - list head
 * oid          - target object ID
 */
int
list_remove(PMEMobjpool *pop,
	size_t pe_offset, struct list_head *head,
	PMEMoid oid)
{
	LOG(3, NULL);
	ASSERTne(head, NULL);

	int ret;
	int out_ret;

	struct lane_section *lane_section;

	if ((ret = lane_hold(pop, &lane_section, LANE_SECTION_LIST))) {
		LOG(2, "lane_hold failed");
		return ret;
	}

	ASSERTne(lane_section, NULL);
	ASSERTne(lane_section->layout, NULL);

	if ((ret = pmemobj_mutex_lock(pop, &head->lock))) {
		LOG(2, "pmemobj_mutex_lock failed");
		goto err;
	}

	struct lane_list_section *section =
		(struct lane_list_section *)lane_section->layout;
	struct redo_log *redo = section->redo;
	size_t redo_index = 0;

	ASSERT((ssize_t)pe_offset >= 0);

	struct list_entry *entry_ptr =
		(struct list_entry *)OBJ_OFF_TO_PTR(pop,
				oid.off + pe_offset);

	struct list_args_remove args = {
		.pe_offset = (ssize_t)pe_offset,
		.head = head,
		.entry_ptr = entry_ptr,
		.obj_doffset = oid.off,
	};

	struct list_args_common args_common = {
		.obj_doffset = oid.off,
		.entry_ptr = entry_ptr,
		.pe_offset = (ssize_t)pe_offset,
	};

	/* remove element from user list */
	redo_index = list_remove_single(pop, redo, redo_index, &args);

	/* clear next and prev offsets in removing element using redo log */
	redo_index = list_fill_entry_redo_log(pop, redo, redo_index,
			&args_common, 0, 0, 0);

	redo_log_set_last(pop, redo, redo_index - 1);

	redo_log_process(pop, redo, REDO_NUM_ENTRIES);

	out_ret = pmemobj_mutex_unlock(pop, &head->lock);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "pmemobj_mutex_unlock failed");
err:
	out_ret = lane_release(pop);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "lane_release failed");

	return ret;
}

/*
 * list_move_oob -- move element between two oob lists
 *
 * pop      - pmemobj pool handle
 * head_old - old list head
 * head_old - new list head
 * oid      - target object ID
 */
int
list_move_oob(PMEMobjpool *pop,
	struct list_head *head_old, struct list_head *head_new,
	PMEMoid oid)
{
	LOG(3, NULL);
	ASSERTne(head_old, NULL);
	ASSERTne(head_new, NULL);

	int ret;
	int out_ret;

	struct lane_section *lane_section;

	if ((ret = lane_hold(pop, &lane_section, LANE_SECTION_LIST))) {
		LOG(2, "lane_hold failed");
		return ret;
	}

	ASSERTne(lane_section, NULL);
	ASSERTne(lane_section->layout, NULL);

	/*
	 * Grab locks in specified order to avoid dead-locks.
	 *
	 * XXX performance improvement: initialize oob locks at pool opening
	 */
	if ((ret = list_mutexes_lock(pop, head_new, head_old))) {
		LOG(2, "list_mutexes_lock failed");
		goto err;
	}

	struct lane_list_section *section =
		(struct lane_list_section *)lane_section->layout;
	struct redo_log *redo = section->redo;
	size_t redo_index = 0;

	uint64_t obj_doffset = oid.off;
	uint64_t obj_offset = obj_doffset - OBJ_OOB_SIZE;

	struct list_entry *entry_ptr =
		(struct list_entry *)OBJ_OFF_TO_PTR(pop, obj_offset
				+ OOB_ENTRY_OFF);

	struct list_args_remove args_remove = {
		.pe_offset = OOB_ENTRY_OFF_REV,
		.head = head_old,
		.entry_ptr = entry_ptr,
		.obj_doffset = obj_doffset,
	};

	struct list_args_common args_common = {
		.obj_doffset = obj_doffset,
		.entry_ptr = entry_ptr,
		.pe_offset = OOB_ENTRY_OFF_REV,
	};

	uint64_t next_offset;
	uint64_t prev_offset;

	/* remove element from oob list */
	redo_index = list_remove_single(pop, redo, redo_index, &args_remove);

	/* insert element to new oob list */
	redo_index = list_insert_oob(pop, redo, redo_index,
			head_new, obj_doffset, &next_offset, &prev_offset);

	/* change next and prev offsets of moving element using redo log */
	redo_index = list_fill_entry_redo_log(pop, redo, redo_index,
			&args_common, next_offset, prev_offset, 0);

	redo_log_set_last(pop, redo, redo_index - 1);

	redo_log_process(pop, redo, REDO_NUM_ENTRIES);

	out_ret = list_mutexes_unlock(pop, head_new, head_old);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "list_mutexes_unlock failed");
err:
	out_ret = lane_release(pop);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "lane_release failed");

	return ret;
}

/*
 * list_move -- move object between two lists
 *
 * pop           - pmemobj handle
 * pe_offset_old - offset to old list entry relative to user data
 * head_old      - old list head
 * pe_offset_new - offset to new list entry relative to user data
 * head_new      - new list head
 * dest          - destination object ID
 * before        - before/after destination
 * oid           - target object ID
 */
int
list_move(PMEMobjpool *pop,
	size_t pe_offset_old, struct list_head *head_old,
	size_t pe_offset_new, struct list_head *head_new,
	PMEMoid dest, int before, PMEMoid oid)
{
	LOG(3, NULL);
	ASSERTne(head_old, NULL);
	ASSERTne(head_new, NULL);

	int ret;
	int out_ret;

	struct lane_section *lane_section;

	if ((ret = lane_hold(pop, &lane_section, LANE_SECTION_LIST))) {
		LOG(2, "lane_hold failed");
		return ret;
	}

	ASSERTne(lane_section, NULL);
	ASSERTne(lane_section->layout, NULL);

	/*
	 * Grab locks in specified order to avoid dead-locks.
	 *
	 * XXX performance improvement: initialize oob locks at pool opening
	 */
	if ((ret = list_mutexes_lock(pop, head_new, head_old))) {
		LOG(2, "list_mutexes_lock failed");
		goto err;
	}

	struct lane_list_section *section =
		(struct lane_list_section *)lane_section->layout;
	struct redo_log *redo = section->redo;
	size_t redo_index = 0;

	dest = list_get_dest(pop, head_new, dest, pe_offset_new, before);

	struct list_entry *entry_ptr_old =
		(struct list_entry *)OBJ_OFF_TO_PTR(pop,
				oid.off + pe_offset_old);

	struct list_entry *entry_ptr_new =
		(struct list_entry *)OBJ_OFF_TO_PTR(pop,
				oid.off + pe_offset_new);

	struct list_entry *dest_entry_ptr =
		(struct list_entry *)OBJ_OFF_TO_PTR(pop,
				dest.off + pe_offset_new);

	ASSERT((ssize_t)pe_offset_old >= 0);
	struct list_args_remove args_remove = {
		.pe_offset = (ssize_t)pe_offset_old,
		.head = head_old,
		.entry_ptr = entry_ptr_old,
		.obj_doffset = oid.off,
	};

	struct list_args_insert args_insert = {
		.head = head_new,
		.dest = dest,
		.dest_entry_ptr = dest_entry_ptr,
		.before = before,
	};

	ASSERT((ssize_t)pe_offset_new >= 0);
	struct list_args_common args_common = {
		.obj_doffset = oid.off,
		.entry_ptr = entry_ptr_new,
		.pe_offset = (ssize_t)pe_offset_new,
	};

	uint64_t next_offset;
	uint64_t prev_offset;

	/* remove element from user list */
	redo_index = list_remove_single(pop, redo, redo_index, &args_remove);

	/* insert element to user list */
	redo_index = list_insert_user(pop, redo, redo_index, &args_insert,
			&args_common, &next_offset, &prev_offset);

	/* offsets differ, move is between different list entries - set uuid */
	int set_uuid = pe_offset_new != pe_offset_old ? 1 : 0;

	/* fill next and prev offsets of moving element using redo log */
	redo_index = list_fill_entry_redo_log(pop, redo, redo_index,
			&args_common, next_offset, prev_offset, set_uuid);

	redo_log_set_last(pop, redo, redo_index - 1);

	redo_log_process(pop, redo, REDO_NUM_ENTRIES);

	out_ret = list_mutexes_unlock(pop, head_new, head_old);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "list_mutexes_unlock failed");
err:
	out_ret = lane_release(pop);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "lane_release failed");

	return ret;
}

/*
 * list_realloc -- realloc list member
 *
 * pop          - pmemobj pool handle
 * oob_head     - oob list head
 * pe_offset    - offset to list entry on user list relative to user data
 * head         - user list head
 * size         - size of allocation, will be increased by OBJ_OOB_SIZE
 * constructor  - object's constructor
 * arg          - argument for object's constructor
 * field_offset - offset to user's field
 * field_value  - user's field value
 * oidp         - pointer to target object ID
 */
int
list_realloc(PMEMobjpool *pop, struct list_head *oob_head,
	size_t pe_offset, struct list_head *head,
	size_t size, void (*constructor)(PMEMobjpool *pop, void *ptr,
	void *arg), void *arg, uint64_t field_offset, uint64_t field_value,
	PMEMoid *oidp)
{
	LOG(3, NULL);
	ASSERTne(oob_head, NULL);
	ASSERTne(oidp, NULL);
	ASSERTne(constructor, NULL);

	int ret;
	int out_ret;

	struct lane_section *lane_section;

	if ((ret = lane_hold(pop, &lane_section, LANE_SECTION_LIST))) {
		LOG(2, "lane_hold failed");
		return ret;
	}

	ASSERTne(lane_section, NULL);
	ASSERTne(lane_section->layout, NULL);

	/*
	 * In case of oob list and user list grab the oob list lock
	 * first.
	 *
	 * XXX performance improvement: initialize oob locks at pool opening
	 */
	if ((ret = pmemobj_mutex_lock(pop, &oob_head->lock))) {
		LOG(2, "pmemobj_mutex_lock failed");
		goto err_oob_lock;
	}

	if (head) {
		if ((ret = pmemobj_mutex_lock(pop, &head->lock))) {
			LOG(2, "pmemobj_mutex_lock failed");
			goto err_lock;
		}
	}

	/* increase allocation size by oob header size */
	size += OBJ_OOB_SIZE;
	struct lane_list_section *section =
		(struct lane_list_section *)lane_section->layout;
	struct redo_log *redo = section->redo;
	size_t redo_index = 0;
	uint64_t obj_doffset = oidp->off;
	uint64_t obj_offset = obj_doffset - OBJ_OOB_SIZE;
	uint64_t old_size = pmalloc_usable_size(pop, obj_offset);
	uint64_t sec_off_off = OBJ_PTR_TO_OFF(pop, &section->obj_offset);

	/*
	 * The following steps must be in consistency with the recovery
	 * process:
	 *
	 * 1. Set the old size field in lane section.
	 * 2. Set the allocation's offset field in lane section.
	 * 3. Perform realloc.
	 * 4. Clear the size field using redo log
	 * 5. Clear the offset field using redo log.
	 * 6. Process the redo log.
	 */
	section->obj_size = old_size;
	pop->persist(pop, &section->obj_size, sizeof (section->obj_size));

	section->obj_offset = obj_offset;
	pop->persist(pop, &section->obj_offset, sizeof (section->obj_offset));

	/*
	 * The user must be aware that any changes in
	 * old area when reallocating in place won't be
	 * made atomically.
	 */
	ret = prealloc_construct(pop,
			&section->obj_offset, size,
			constructor, arg, OBJ_OOB_SIZE);

	if (!ret) {
		uint64_t sec_size_off = OBJ_PTR_TO_OFF(pop, &section->obj_size);

		/* clear offset and size field in lane section */
		redo_log_store(pop, redo, 0, sec_size_off, 0);
		redo_log_store(pop, redo, 1, sec_off_off, 0);

		if (field_offset)
			/* set user's field */
			redo_log_store_last(pop, redo, 2,
					field_offset, field_value);
		else
			redo_log_set_last(pop, redo, 1);

		redo_log_process(pop, redo, REDO_NUM_ENTRIES);
	} else {
		/*
		 * If realloc in-place failed clear the obj_offset and obj_size.
		 */
		section->obj_offset = 0;
		pop->persist(pop, &section->obj_offset,
				sizeof (section->obj_offset));

		section->obj_size = 0;
		pop->persist(pop, &section->obj_size,
				sizeof (section->obj_size));

		/*
		 * Realloc in place is not possible so we need to perform
		 * the following steps:
		 *
		 * 1. Allocate memory with new size.
		 * 2. Memcpy.
		 * 3. Reinsert the new element to first list using redo log.
		 * 4. Reinsert the new element to second list using redo log.
		 * 5. Optionally set the user's field using redo log.
		 * 6. Set the offset field in section to old allocation using
		 *    redo log.
		 * 7. Process the redo log.
		 * 8. Free the old allocation.
		 */
		ret = pmalloc(pop, &section->obj_offset, size, OBJ_OOB_SIZE);
		if (ret) {
			errno = ret;
			ERR("!pmalloc");
			ret = -1;
			goto err_unlock;
		}

		uint64_t new_obj_offset = section->obj_offset;
		uint64_t new_obj_doffset = new_obj_offset + OBJ_OOB_SIZE;

		redo_index = list_realloc_replace(pop,
				redo, redo_index,
				head, pe_offset, old_size,
				obj_offset, new_obj_offset,
				constructor, arg,
				field_offset, field_value);

		struct list_entry *oob_entry_ptr =
			(struct list_entry *)OBJ_OFF_TO_PTR(pop,
					obj_offset + OOB_ENTRY_OFF);

		struct list_entry *oob_new_entry_ptr =
			(struct list_entry *)OBJ_OFF_TO_PTR(pop,
					new_obj_offset + OOB_ENTRY_OFF);

		struct list_args_reinsert oob_args_reinsert = {
			.head = oob_head,
			.entry_ptr = oob_entry_ptr,
			.obj_doffset = obj_doffset,
		};

		struct list_args_common oob_args_common = {
			.obj_doffset = new_obj_doffset,
			.entry_ptr = oob_new_entry_ptr,
			.pe_offset = OOB_ENTRY_OFF_REV,
		};

		uint64_t next_offset;
		uint64_t prev_offset;

		/* replace new item in first list */
		redo_index = list_replace_single(pop, redo, redo_index,
				&oob_args_reinsert, &oob_args_common,
				&next_offset, &prev_offset);

		/* fill next and prev offsets of new entry without redo log */
		list_fill_entry_persist(pop, oob_new_entry_ptr,
				next_offset, prev_offset);

		if (OBJ_PTR_IS_VALID(pop, oidp))
			redo_index = list_set_oid_redo_log(pop, redo,
					redo_index, oidp, new_obj_doffset, 1);
		else
			oidp->off = new_obj_doffset;

		/* set offset for pfree */
		redo_log_store_last(pop, redo, redo_index,
				sec_off_off, obj_offset);

		redo_log_process(pop, redo, REDO_NUM_ENTRIES);

		/* free the old object */
		if ((ret = pfree(pop, &section->obj_offset, OBJ_OOB_SIZE))) {
			errno = ret;
			ERR("!pfree");
			ret = -1;
			goto err_unlock;
		}
	}

	ret = 0;
err_unlock:
	if (head) {
		out_ret = pmemobj_mutex_unlock(pop, &head->lock);
		ASSERTeq(out_ret, 0);
		if (out_ret)
			LOG(2, "pmemobj_mutex_unlock failed");
	}

err_lock:
	out_ret = pmemobj_mutex_unlock(pop, &oob_head->lock);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "pmemobj_mutex_unlock failed");
err_oob_lock:
	out_ret = lane_release(pop);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "lane_release failed");

	return ret;
}

/*
 * list_realloc_move -- realloc and move an element
 * pop          - pmemobj pool handle
 * oob_head_old - old oob list head
 * oob_head_new - new oob list head
 * pe_offset    - offset to list entry on user list relative to user data
 * head         - user list head
 * size         - size of allocation, will be increased by OBJ_OOB_SIZE
 * constructor  - object's constructor
 * arg          - argument for object's constructor
 * field_offset - offset to user's field
 * field_value  - user's field value
 * oidp         - pointer to target object ID
 */
int
list_realloc_move(PMEMobjpool *pop, struct list_head *oob_head_old,
	struct list_head *oob_head_new, size_t pe_offset,
	struct list_head *head, size_t size,
	void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg), void *arg,
	uint64_t field_offset, uint64_t field_value,
	PMEMoid *oidp)
{
	LOG(3, NULL);

	ASSERTne(oob_head_old, NULL);
	ASSERTne(oob_head_new, NULL);
	ASSERTne(constructor, NULL);

	int ret;
	int out_ret;

	struct lane_section *lane_section;

	if ((ret = lane_hold(pop, &lane_section, LANE_SECTION_LIST))) {
		LOG(2, "lane_hold failed");
		return ret;
	}

	ASSERTne(lane_section, NULL);
	ASSERTne(lane_section->layout, NULL);

	/* increase allocation size by oob header size */
	size += OBJ_OOB_SIZE;
	struct lane_list_section *section =
		(struct lane_list_section *)lane_section->layout;
	struct redo_log *redo = section->redo;
	size_t redo_index = 0;

	/*
	 * In case of oob lists and user list grab the oob list locks
	 * first. The oob list locks are grabbed in specified order.
	 *
	 * XXX performance improvement: initialize oob locks at pool opening
	 */
	if ((ret = list_mutexes_lock(pop, oob_head_new, oob_head_old)) < 0) {
		LOG(2, "list_mutexes_lock failed");
		goto err_oob_lock;
	}

	if (head) {
		if ((ret = pmemobj_mutex_lock(pop, &head->lock))) {
			LOG(2, "pmemobj_mutex_lock failed");
			goto err_lock;
		}
	}

	uint64_t obj_doffset = oidp->off;
	uint64_t obj_offset = obj_doffset - OBJ_OOB_SIZE;
	uint64_t new_obj_doffset = obj_doffset;
	uint64_t new_obj_offset = obj_offset;
	uint64_t old_size = pmalloc_usable_size(pop, obj_offset);
	uint64_t sec_off_off = OBJ_PTR_TO_OFF(pop, &section->obj_offset);
	int in_place = 0;

	/*
	 * The following steps may be in consistency with the recovery
	 * process:
	 *
	 * 1. Set the old size field in lane section.
	 * 2. Set the allocation's offset field in lane section.
	 * 3. Perform realloc.
	 * 4. Clear the size field using redo log
	 * 5. Clear the offset field using redo log.
	 * 6. Process the redo log.
	 */
	section->obj_size = old_size;
	pop->persist(pop, &section->obj_size, sizeof (section->obj_size));

	section->obj_offset = obj_offset;
	pop->persist(pop, &section->obj_offset, sizeof (section->obj_offset));

	/*
	 * The user must be aware that any changes in
	 * old area when reallocating in place won't be
	 * made atomically.
	 */
	ret = prealloc_construct(pop,
			&section->obj_offset, size,
			constructor, arg, OBJ_OOB_SIZE);

	if (!ret) {
		uint64_t sec_size_off = OBJ_PTR_TO_OFF(pop, &section->obj_size);

		/* clear offset and size field in lane section */
		redo_log_store(pop, redo, redo_index + 0, sec_size_off, 0);
		redo_log_store(pop, redo, redo_index + 1, sec_off_off, 0);
		redo_index += 2;

		/* set user's field */
		if (field_offset) {
			redo_log_store(pop, redo, redo_index + 0,
					field_offset, field_value);
			redo_index += 1;
		}

		in_place = 1;
	} else {
		/*
		 * If realloc in-place failed clear the obj_offset and obj_size.
		 */
		section->obj_offset = 0;
		pop->persist(pop, &section->obj_offset,
				sizeof (section->obj_offset));

		section->obj_size = 0;
		pop->persist(pop, &section->obj_size,
				sizeof (section->obj_size));

		/*
		 * Realloc in place is not possible so we need to perform
		 * the following steps:
		 *
		 * 1. Allocate memory with new size.
		 * 2. Memcpy.
		 * 3. Reinsert the new element to first list using redo log.
		 * 4. Reinsert the new element to second list using redo log.
		 * 5. Optionally set the user's field using redo log.
		 * 6. Set the offset field in section to old allocation using
		 *    redo log.
		 * 7. Process the redo log.
		 * 8. Free the old allocation.
		 */
		ret = pmalloc(pop, &section->obj_offset, size, OBJ_OOB_SIZE);
		if (ret) {
			errno = ret;
			ERR("!pmalloc");
			ret = -1;
			goto err_unlock;
		}

		new_obj_offset = section->obj_offset;
		new_obj_doffset = new_obj_offset + OBJ_OOB_SIZE;

		redo_index = list_realloc_replace(pop,
				redo, redo_index,
				head, pe_offset, old_size,
				obj_offset, new_obj_offset,
				constructor, arg,
				field_offset, field_value);

		/* set offset for pfree */
		redo_log_store(pop, redo, redo_index,
				sec_off_off, obj_offset);
		redo_index += 1;

		if (OBJ_PTR_IS_VALID(pop, oidp))
			redo_index = list_set_oid_redo_log(pop, redo,
					redo_index, oidp, new_obj_doffset, 1);
		else
			oidp->off = new_obj_doffset;
	}

	struct list_entry *entry_ptr_old =
		(struct list_entry *)OBJ_OFF_TO_PTR(pop,
				obj_offset + OOB_ENTRY_OFF);
	struct list_entry *entry_ptr_new =
		(struct list_entry *)OBJ_OFF_TO_PTR(pop,
				new_obj_offset + OOB_ENTRY_OFF);

	struct list_args_remove args_remove = {
		.pe_offset = OOB_ENTRY_OFF_REV,
		.head = oob_head_old,
		.entry_ptr = entry_ptr_old,
		.obj_doffset = obj_doffset,
	};

	struct list_args_common args_common = {
		.obj_doffset = new_obj_doffset,
		.entry_ptr = entry_ptr_new,
		.pe_offset = OOB_ENTRY_OFF_REV,
	};

	uint64_t next_offset;
	uint64_t prev_offset;

	/* remove from old oob list */
	redo_index = list_remove_single(pop, redo, redo_index, &args_remove);

	/* insert to new oob list */
	redo_index = list_insert_oob(pop, redo, redo_index, oob_head_new,
			new_obj_doffset, &next_offset, &prev_offset);

	if (in_place) {
		/* realloc in place so we need to use redo log */
		redo_index = list_fill_entry_redo_log(pop, redo, redo_index,
				&args_common, next_offset, prev_offset, 0);
	} else {
		/*
		 * Realloc not in place so no need to modify next and prev
		 * offsets using redo log.
		 */
		list_fill_entry_persist(pop, entry_ptr_new,
				next_offset, prev_offset);
	}

	redo_log_set_last(pop, redo, redo_index - 1);

	redo_log_process(pop, redo, REDO_NUM_ENTRIES);

	if (!in_place) {
		ASSERTne(section->obj_offset, 0);

		/* realloc not in place so free the old object */
		if ((ret = pfree(pop, &section->obj_offset, OBJ_OOB_SIZE))) {
			errno = ret;
			ERR("!pfree");
			ret = -1;
			goto err_unlock;
		}
	}

	ret = 0;
err_unlock:
	if (head) {
		out_ret = pmemobj_mutex_unlock(pop, &head->lock);
		ASSERTeq(out_ret, 0);
		if (out_ret)
			LOG(2, "pmemobj_mutex_unlock failed");
	}
err_lock:
	out_ret = list_mutexes_unlock(pop, oob_head_new, oob_head_old);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "list_mutexes_unlock failed");
err_oob_lock:
	out_ret = lane_release(pop);
	ASSERTeq(out_ret, 0);
	if (out_ret)
		LOG(2, "lane_release failed");

	return ret;
}

/*
 * lane_list_recovery -- (internal) recover the list section of the lane
 */
static int
lane_list_recovery(PMEMobjpool *pop, struct lane_section_layout *section_layout)
{
	LOG(3, "list lane %p", section_layout);

	struct lane_list_section *section =
		(struct lane_list_section *)section_layout;

	int ret = 0;

	redo_log_recover(pop, section->redo, REDO_NUM_ENTRIES);

	if (section->obj_size) {
		/* realloc recovery */
		if (section->obj_offset) {
			size_t size = pmalloc_usable_size(pop,
					section->obj_offset);
			if (size != section->obj_size) {
				/*
				 * If both size and offset are non-zero and the
				 * real allocation size is different than
				 * stored in lane section it means the realloc
				 * was performed but the finish flag was not set
				 * so we need to rollback the realloc.
				 */
				ret = prealloc(pop, &section->obj_offset, size,
						OBJ_OOB_SIZE);
				if (ret) {
					errno = ret;
					ERR("!prealloc");
					goto err;
				}
			}
			/*
			 * Size and offset was set but the realloc was not made
			 * so just clear the offset and size.
			 */
			section->obj_offset = 0;
			pop->persist(pop, &section->obj_offset,
					sizeof (section->obj_offset));
		}
		/*
		 * The size was set but offset was not set so just clear the
		 * size field.
		 */
		section->obj_size = 0;
		pop->persist(pop, &section->obj_size,
				sizeof (section->obj_size));

	} else if (section->obj_offset) {
		/* alloc or free recovery */
		if ((ret = pfree(pop, &section->obj_offset, OBJ_OOB_SIZE))) {
			errno = ret;
			ERR("!pfree");
			ret = -1;
		}
	}
err:
	return ret;
}

/*
 * lane_list_check -- (internal) check consistency of lane
 */
static int
lane_list_check(PMEMobjpool *pop, struct lane_section_layout *section_layout)
{
	LOG(3, "list lane %p", section_layout);

	struct lane_list_section *section =
		(struct lane_list_section *)section_layout;

	int ret = 0;
	if ((ret = redo_log_check(pop,
			section->redo, REDO_NUM_ENTRIES)) != 0) {
		ERR("list lane: redo log check failed");

		return ret;
	}

	if (section->obj_offset &&
		!OBJ_OFF_FROM_HEAP(pop, section->obj_offset)) {
		ERR("list lane: invalid offset 0x%jx",
				section->obj_offset);

		return -1;
	}

	return 0;
}

/*
 * lane_list_construct -- (internal) create list lane section
 */
static int
lane_list_construct(PMEMobjpool *pop, struct lane_section *section)
{
	/* nop */
	return 0;
}

/*
 * lane_list_destruct -- (internal) destroy list lane section
 */
static int
lane_list_destruct(PMEMobjpool *pop, struct lane_section *section)
{
	/* nop */
	return 0;
}

/*
 * lane_list_init -- initializes list section
 */
static int
lane_list_boot(PMEMobjpool *pop)
{
	/* nop */
	return 0;
}

static struct section_operations list_ops = {
	.construct = lane_list_construct,
	.destruct = lane_list_destruct,
	.recover = lane_list_recovery,
	.check = lane_list_check,
	.boot = lane_list_boot
};

SECTION_PARM(LANE_SECTION_LIST, &list_ops);
