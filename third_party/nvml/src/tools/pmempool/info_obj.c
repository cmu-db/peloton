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
 * info_obj.c -- pmempool info command source file for obj pool
 */
#include <stdlib.h>
#include <stdbool.h>
#include <err.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <assert.h>

#include "common.h"
#include "output.h"
#include "info.h"

#define	BITMAP_BUFF_SIZE 1024
#define	TYPE_NUM_BUFF_SIZE 32

#define	OFF_TO_PTR(pop, off) ((void *)((uintptr_t)(pop) + (off)));

#define	PTR_TO_OFF(pop, ptr) ((uintptr_t)ptr - (uintptr_t)pop)


typedef void (*list_callback_fn)(struct pmem_info *pip, int v, int vnum,
		struct pmemobjpool *pop, struct list_entry *entry, size_t i);

/*
 * lane_need_recovery_redo -- return 1 if redo log needs recovery
 */
static int
lane_need_recovery_redo(struct redo_log *redo, size_t nentries)
{
	/* Needs recovery if any of redo log entries has finish flag set */
	for (size_t i = 0; i < nentries; i++) {
		if (redo[i].offset & REDO_FINISH_FLAG)
			return 1;
	}

	return 0;
}

/*
 * lane_need_recovery_list -- return 1 if list's section needs recovery
 */
static int
lane_need_recovery_list(struct lane_section_layout *layout)
{
	struct lane_list_section *section = (struct lane_list_section *)layout;

	/*
	 * The list section needs recovery if redo log needs recovery or
	 * object's offset or size are nonzero.
	 */
	return lane_need_recovery_redo(&section->redo[0], REDO_NUM_ENTRIES) ||
		section->obj_offset ||
		section->obj_size;
}

/*
 * lane_need_recovery_alloc -- return 1 if allocator's section needs recovery
 */
static int
lane_need_recovery_alloc(struct lane_section_layout *layout)
{
	struct allocator_lane_section *section =
		(struct allocator_lane_section *)layout;

	/* there is just a redo log */
	return lane_need_recovery_redo(&section->redo[0], REDO_LOG_SIZE);
}

/*
 * lane_need_recovery_tx -- return 1 if transaction's section needs recovery
 */
static int
lane_need_recovery_tx(struct lane_section_layout *layout)
{
	struct lane_tx_layout *section = (struct lane_tx_layout *)layout;

	/*
	 * The transaction section needs recovery
	 * if state is not committed and
	 * any undo log not empty
	 */
	return section->state == TX_STATE_NONE &&
		(!PLIST_EMPTY(&section->undo_alloc) ||
		!PLIST_EMPTY(&section->undo_free) ||
		!PLIST_EMPTY(&section->undo_set));
}

/*
 * lane_need_recovery -- return 1 if lane section needs recovery
 */
static int
lane_need_recovery(struct lane_layout *lane)
{
	int alloc = lane_need_recovery_alloc(
			&lane->sections[LANE_SECTION_ALLOCATOR]);
	int list = lane_need_recovery_list(
			&lane->sections[LANE_SECTION_LIST]);
	int tx = lane_need_recovery_tx(
			&lane->sections[LANE_SECTION_TRANSACTION]);

	return alloc || list || tx;
}

/*
 * heap_size_to_class -- get index of class of given allocation size
 */
static int
heap_size_to_class(size_t size)
{
	if (!size)
		return -1;

	if (size == CHUNKSIZE)
		return DEFAULT_BUCKET;

	int class = 0;

	while (size != MIN_RUN_SIZE) {
		size = size / RUN_UNIT_MAX;
		class++;
	}

	return class;
}

/*
 * heal_class_to_size -- get size of allocation class
 */
static uint64_t
heap_class_to_size(int class)
{
	if (class == DEFAULT_BUCKET)
		return CHUNKSIZE;

	uint64_t size = MIN_RUN_SIZE;

	for (int i = 0; i < class; i++)
		size = size * RUN_UNIT_MAX;

	return size;
}

/*
 * get_bitmap_size -- get number of used bits in chunk run's bitmap
 */
static uint32_t
get_bitmap_size(struct chunk_run *run)
{
	uint64_t size = RUNSIZE / run->block_size;
	assert(size <= UINT32_MAX);
	return (uint32_t)size;
}

/*
 * get_bitmap_reserved -- get number of reserved blocks in chunk run
 */
static int
get_bitmap_reserved(struct chunk_run *run, uint32_t *reserved)
{
	uint64_t nvals = 0;
	uint64_t last_val = 0;
	if (util_heap_get_bitmap_params(run->block_size, NULL, &nvals,
			&last_val))
		return -1;

	uint32_t ret = 0;
	for (uint64_t i = 0; i < nvals - 1; i++)
		ret += util_count_ones(run->bitmap[i]);
	ret += util_count_ones(run->bitmap[nvals - 1] & ~last_val);

	*reserved = ret;

	return 0;
}

/*
 * get_bitmap_str -- get bitmap single value string
 */
static const char *
get_bitmap_str(uint64_t val, unsigned values)
{
	static char buff[BITMAP_BUFF_SIZE];

	unsigned j = 0;
	for (unsigned i = 0; i < values && j < BITMAP_BUFF_SIZE - 3; i++) {
		buff[j++] = ((val & ((uint64_t)1 << i)) ? 'x' : '.');
		if ((i + 1) % RUN_UNIT_MAX == 0)
			buff[j++] = ' ';
	}

	buff[j] = '\0';

	return buff;
}

/*
 * info_obj_redo -- print redo log entries
 */
static void
info_obj_redo(int v, struct redo_log *redo, size_t nentries)
{
	outv_field(v, "Redo log entries", "%lu", nentries);
	for (size_t i = 0; i < nentries; i++) {
		outv(v, "%010u: "
			"Offset: 0x%016jx "
			"Value: 0x%016jx "
			"Finish flag: %d\n",
			i,
			redo[i].offset & REDO_FLAG_MASK,
			redo[i].value,
			redo[i].offset & REDO_FINISH_FLAG);
	}
}

/*
 * info_obj_lane_alloc -- print allocator's lane section
 */
static void
info_obj_lane_alloc(int v, struct lane_section_layout *layout)
{
	struct allocator_lane_section *section =
		(struct allocator_lane_section *)layout;
	info_obj_redo(v, &section->redo[0], REDO_LOG_SIZE);
}

/*
 * info_obj_lane_list -- print list's lane section
 */
static void
info_obj_lane_list(struct pmem_info *pip, int v,
		struct lane_section_layout *layout)
{
	struct lane_list_section *section = (struct lane_list_section *)layout;

	outv_field(v, "Object offset", "0x%016lx", section->obj_offset);
	outv_field(v, "Object size", "%s",
			out_get_size_str(section->obj_size, pip->args.human));
	info_obj_redo(v, &section->redo[0], REDO_NUM_ENTRIES);
}

/*
 * info_obj_list -- iterate through all elements on list and invoke
 * callback function for each element
 */
static void
info_obj_list(struct pmem_info *pip, int v, int vnum, struct pmemobjpool *pop,
	struct list_head *headp, const char *name, list_callback_fn cb)
{
	size_t nelements = util_plist_nelements(pop, headp);
	if (pip->args.obj.ignore_empty_obj && nelements == 0)
		return;

	outv_field(v, name, "%lu element%s",
				nelements, nelements != 1 ? "s" : "");
	out_indent(1);
	size_t i = 0;
	struct list_entry *entryp;
	PLIST_FOREACH(entryp, pop, headp) {
		cb(pip, v, vnum, pop, entryp, i);
		i++;
	}
	out_indent(-1);
}

/*
 * info_obj_oob_hdr -- print OOB header
 */
static void
info_obj_oob_hdr(struct pmem_info *pip, int v, struct pmemobjpool *pop,
		struct oob_header *oob)
{
	outv_title(v, "OOB Header");
	outv_hexdump(v && pip->args.vhdrdump, oob, sizeof (*oob),
		PTR_TO_OFF(pop, oob), 1);
	outv_field(v, "Next",
		out_get_pmemoid_str(oob->oob.pe_next, pip->obj.uuid_lo));
	outv_field(v, "Prev",
		out_get_pmemoid_str(oob->oob.pe_prev, pip->obj.uuid_lo));
	outv_field(v, "Internal Type", "%s",
		out_get_internal_type_str(oob->data.internal_type));

	if (oob->data.user_type == POBJ_ROOT_TYPE_NUM)
		outv_field(v, "User Type", "%u [root object]",
				oob->data.user_type);
	else
		outv_field(v, "User Type", "%u", oob->data.user_type);
}

/*
 * info_obj_alloc_hdr -- print allocation header
 */
static void
info_obj_alloc_hdr(struct pmem_info *pip, int v,
		struct allocation_header *alloc)
{
	outv_title(v, "Allocation Header");
	outv_hexdump(v && pip->args.vhdrdump, alloc,
			sizeof (*alloc), PTR_TO_OFF(pip->obj.addr, alloc), 1);
	outv_field(v, "Zone id", "%u", alloc->zone_id);
	outv_field(v, "Chunk id", "%u", alloc->chunk_id);
	outv_field(v, "Size", "%s",
			out_get_size_str(alloc->size, pip->args.human));
}

/*
 * obj_object_cb -- callback for objects
 */
static void
obj_object_cb(struct pmem_info *pip, int v, int vnum, struct pmemobjpool *pop,
		struct list_entry *entryp, size_t i)
{
	struct oob_header *oob = ENTRY_TO_OOB_HDR(entryp);
	struct allocation_header *alloc = ENTRY_TO_ALLOC_HDR(entryp);
	void *data = ENTRY_TO_DATA(entryp);

	outv_nl(vnum);
	outv_field(vnum, "Object", "%lu", i);
	outv_field(vnum, "Offset", "0x%016lx", PTR_TO_OFF(pop, data));

	out_indent(1);
	info_obj_alloc_hdr(pip, v && pip->args.obj.valloc, alloc);
	info_obj_oob_hdr(pip, v && pip->args.obj.voobhdr, pop, oob);

	outv_hexdump(v && pip->args.vdata, data,
			alloc->size, PTR_TO_OFF(pip->obj.addr, data), 1);
	outv_nl(vnum);
	out_indent(-1);
}

/*
 * set_entry_cb -- callback for set objects from undo log
 */
static void
set_entry_cb(struct pmem_info *pip, int v, int vnum, struct pmemobjpool *pop,
		struct list_entry *entryp, size_t i)
{
	struct tx_range *range = ENTRY_TO_TX_RANGE(entryp);
	obj_object_cb(pip, v, vnum, pop, entryp, i);

	outv_title(vnum, "Tx range");
	outv_field(vnum, "Offset", "0x%016lx", range->offset);
	outv_field(vnum, "Size", "%s",
			out_get_size_str(range->size, pip->args.human));
}

/*
 * info_obj_lane_tx -- print transaction's lane section
 */
static void
info_obj_lane_tx(struct pmem_info *pip, int v, struct pmemobjpool *pop,
		struct lane_section_layout *layout)
{
	struct lane_tx_layout *section = (struct lane_tx_layout *)layout;

	outv_field(v, "State", "%s", out_get_tx_state_str(section->state));

	int vnum = v && (pip->args.obj.valloc || pip->args.obj.voobhdr);
	info_obj_list(pip, v, vnum, pop, &section->undo_alloc,
			"Undo Log - alloc", obj_object_cb);
	info_obj_list(pip, v, vnum, pop, &section->undo_free,
			"Undo Log - free", obj_object_cb);
	info_obj_list(pip, v, vnum, pop, &section->undo_set,
			"Undo Log - set", set_entry_cb);
}

/*
 * info_obj_lane_section -- print lane's section
 */
static void
info_obj_lane_section(struct pmem_info *pip, int v, struct pmemobjpool *pop,
	struct lane_layout *lane, enum lane_section_type type)
{
	if (!(pip->args.obj.lane_sections & (1U << type)))
		return;

	outv_nl(v);
	outv_field(v, "Lane section", "%s",
		out_get_lane_section_str(type));
	outv_hexdump(v && pip->args.vhdrdump,
			&lane->sections[type],
			LANE_SECTION_LEN,
			PTR_TO_OFF(pop, &lane->sections[type]),
			1);

	out_indent(1);
	switch (type) {
	case LANE_SECTION_ALLOCATOR:
		info_obj_lane_alloc(v, &lane->sections[type]);
		break;
	case LANE_SECTION_LIST:
		info_obj_lane_list(pip, v, &lane->sections[type]);
		break;
	case LANE_SECTION_TRANSACTION:
		info_obj_lane_tx(pip, v, pop, &lane->sections[type]);
		break;
	default:
		break;
	}
	out_indent(-1);
}

/*
 * info_obj_lanes -- print lanes structures
 */
static void
info_obj_lanes(struct pmem_info *pip, int v,
		struct pmemobjpool *pop)
{
	if (!outv_check(v))
		return;

	/*
	 * Iterate through all lanes from specified range and print
	 * specified sections.
	 */
	struct lane_layout *lanes = (void *)((char *)pip->obj.addr +
			pop->lanes_offset);
	uint64_t i;
	struct range *curp = NULL;
	FOREACH_RANGE(curp, &pip->args.obj.lane_ranges) {
		for (i = curp->first;
			i <= curp->last && i < pop->nlanes; i++) {

			/* For -R check print lane only if needs recovery */
			if (pip->args.obj.lanes_recovery &&
				!lane_need_recovery(&lanes[i]))
				continue;

			outv_title(v, "Lane", "%d", i);

			out_indent(1);
			info_obj_lane_section(pip, v, pop, &lanes[i],
					LANE_SECTION_ALLOCATOR);
			info_obj_lane_section(pip, v, pop, &lanes[i],
					LANE_SECTION_LIST);
			info_obj_lane_section(pip, v, pop, &lanes[i],
					LANE_SECTION_TRANSACTION);
			out_indent(-1);
		}
	}
}

/*
 * info_obj_store_object_cb -- callback for objects from object store
 */
static void
info_obj_store_object_cb(struct pmem_info *pip, int v, int vnum,
		struct pmemobjpool *pop, struct list_entry *entryp, size_t i)
{
	struct allocation_header *alloc = ENTRY_TO_ALLOC_HDR(entryp);
	struct oob_header *oob = ENTRY_TO_OOB_HDR(entryp);
	assert(oob->data.user_type < PMEMOBJ_NUM_OID_TYPES);

	uint64_t real_size = alloc->size -
		sizeof (struct allocation_header) - sizeof (struct oob_header);

	if (!util_ranges_contain(&pip->args.ranges, i))
		return;

	if (!util_ranges_contain(&pip->args.obj.zone_ranges, alloc->zone_id))
		return;

	if (!util_ranges_contain(&pip->args.obj.chunk_ranges, alloc->chunk_id))
		return;

	pip->obj.stats.n_total_objects++;
	pip->obj.stats.n_total_bytes += real_size;

	pip->obj.stats.n_type_objects[oob->data.user_type]++;
	pip->obj.stats.n_type_bytes[oob->data.user_type] += real_size;

	obj_object_cb(pip, v, vnum, pop, entryp, i);
}

/*
 * info_obj_store -- print information about object store
 */
static void
info_obj_store(struct pmem_info *pip, int v, struct pmemobjpool *pop)
{
	if (!outv_check(v) && !outv_check(pip->args.vstats))
		return;

	char buff[TYPE_NUM_BUFF_SIZE];
	struct object_store *obj_store = OFF_TO_PTR(pop, pop->obj_store_offset);

	outv_title(v, "Object store");

	/* verbosity level for the object number */
	int vnum = v && (pip->args.obj.valloc ||
			pip->args.obj.voobhdr ||
			pip->args.vdata);

	uint64_t i;
	struct range *curp;
	FOREACH_RANGE(curp, &pip->args.obj.object_ranges) {
		for (i = curp->first; i <= curp->last &&
				i < PMEMOBJ_NUM_OID_TYPES; i++) {
			snprintf(buff, TYPE_NUM_BUFF_SIZE,
					"Type number %4lu", i);

			info_obj_list(pip, v, vnum, pop,
				&obj_store->bytype[i].head, buff,
				info_obj_store_object_cb);
		}
	}
}

/*
 * info_obj_heap -- print pmemobj heap headers
 */
static void
info_obj_heap(struct pmem_info *pip, int v,
		struct pmemobjpool *pop)
{
	struct heap_layout *layout = OFF_TO_PTR(pop, pop->heap_offset);
	struct heap_header *heap = &layout->header;

	outv(v, "\nPMEMOBJ Heap Header:\n");
	outv_hexdump(v && pip->args.vhdrdump, heap, sizeof (*heap),
			pop->heap_offset, 1);

	outv_field(v, "Signature", "%s", heap->signature);
	outv_field(v, "Major", "%ld", heap->major);
	outv_field(v, "Minor", "%ld", heap->minor);
	outv_field(v, "Size", "%s",
			out_get_size_str(heap->size, pip->args.human));
	outv_field(v, "Chunk size", "%s",
			out_get_size_str(heap->chunksize, pip->args.human));
	outv_field(v, "Chunks per zone", "%ld", heap->chunks_per_zone);
	outv_field(v, "Checksum", "%s", out_get_checksum(heap, sizeof (*heap),
				&heap->checksum));
}

/*
 * info_obj_zone -- print information about zone
 */
static void
info_obj_zone_hdr(struct pmem_info *pip, int v, struct pmemobjpool *pop,
		struct zone_header *zone)
{
	outv_hexdump(v && pip->args.vhdrdump, zone, sizeof (*zone),
			PTR_TO_OFF(pop, zone), 1);
	outv_field(v, "Magic", "%s", out_get_zone_magic_str(zone->magic));
	outv_field(v, "Size idx", "%u", zone->size_idx);
}

/*
 * info_obj_run_bitmap -- print chunk run's bitmap
 */
static void
info_obj_run_bitmap(int v, struct chunk_run *run)
{
	if (outv_check(v) && outv_check(VERBOSE_MAX)) {
		/* print all values from bitmap for higher verbosity */
		for (int i = 0; i < MAX_BITMAP_VALUES; i++) {
			outv(VERBOSE_MAX, "%s\n",
					get_bitmap_str(run->bitmap[i],
						BITS_PER_VALUE));
		}
	} else {
		/* print only used values for lower verbosity */
		uint32_t i;
		for (i = 0; i < get_bitmap_size(run) / BITS_PER_VALUE; i++)
			outv(v, "%s\n", get_bitmap_str(run->bitmap[i],
						BITS_PER_VALUE));

		unsigned mod = get_bitmap_size(run) % BITS_PER_VALUE;
		if (mod != 0) {
			outv(v, "%s\n", get_bitmap_str(run->bitmap[i], mod));
		}
	}
}

/*
 * info_obj_chunk_hdr -- print chunk header
 */
static void
info_obj_chunk_hdr(struct pmem_info *pip, int v, struct pmemobjpool *pop,
		uint64_t c, struct chunk_header *chunk_hdr, struct chunk *chunk,
		struct pmem_obj_zone_stats *stats)
{
	outv(v, "\n");
	outv_field(v, "Chunk", "%lu", c);

	outv_hexdump(v && pip->args.vhdrdump, chunk_hdr, sizeof (*chunk_hdr),
			PTR_TO_OFF(pop, chunk_hdr), 1);

	outv_field(v, "Type", "%s", out_get_chunk_type_str(chunk_hdr->type));
	outv_field(v, "Flags", "0x%x %s", chunk_hdr->flags,
			out_get_chunk_flags(chunk_hdr->flags));
	outv_field(v, "Size idx", "%lu", chunk_hdr->size_idx);
	if (chunk_hdr->type == CHUNK_TYPE_USED ||
		chunk_hdr->type == CHUNK_TYPE_FREE) {
		stats->class_stats[DEFAULT_BUCKET].n_units +=
			chunk_hdr->size_idx;

		if (chunk_hdr->type == CHUNK_TYPE_USED) {
			stats->class_stats[DEFAULT_BUCKET].n_used +=
				chunk_hdr->size_idx;
		}
	} else if (chunk_hdr->type == CHUNK_TYPE_RUN) {
		struct chunk_run *run = (struct chunk_run *)chunk;

		outv_hexdump(v && pip->args.vhdrdump, run,
				sizeof (run->block_size) + sizeof (run->bitmap),
				PTR_TO_OFF(pop, run), 1);

		int class = heap_size_to_class(run->block_size);
		if (class >= 0 && class < MAX_BUCKETS) {
			outv_field(v, "Block size", "%s",
					out_get_size_str(run->block_size,
						pip->args.human));

			uint32_t units = get_bitmap_size(run);
			uint32_t used = 0;
			if (get_bitmap_reserved(run,  &used)) {
				outv_field(v, "Bitmap", "[error]");
			} else {
				stats->class_stats[class].n_units += units;
				stats->class_stats[class].n_used += used;

				outv_field(v, "Bitmap", "%u / %u", used, units);
			}

			info_obj_run_bitmap(v && pip->args.obj.vbitmap, run);
		} else {
			outv_field(v, "Block size", "%s [invalid!]",
					out_get_size_str(run->block_size,
						pip->args.human));
		}
	}
}

/*
 * info_obj_zone_chunks -- print chunk headers from specified zone
 */
static void
info_obj_zone_chunks(struct pmem_info *pip, struct pmemobjpool *pop,
		struct zone *zone, struct pmem_obj_zone_stats *stats)
{
	uint64_t c = 0;
	while (c < zone->header.size_idx) {
		enum chunk_type type = zone->chunk_headers[c].type;
		uint64_t size_idx = zone->chunk_headers[c].size_idx;
		if (util_ranges_contain(&pip->args.obj.chunk_ranges, c)) {
			if (pip->args.obj.chunk_types & (1U << type)) {
				stats->n_chunks++;
				stats->n_chunks_type[type]++;

				stats->size_chunks += size_idx;
				stats->size_chunks_type[type] += size_idx;

				info_obj_chunk_hdr(pip, pip->args.obj.vchunkhdr,
						pop, c,
						&zone->chunk_headers[c],
						&zone->chunks[c], stats);

			}
			if (size_idx > 1 && type != CHUNK_TYPE_RUN &&
				pip->args.obj.chunk_types &
				(1 << CHUNK_TYPE_FOOTER)) {
				size_t f = c + size_idx - 1;
				info_obj_chunk_hdr(pip, pip->args.obj.vchunkhdr,
					pop, f,
					&zone->chunk_headers[f],
					&zone->chunks[f], stats);
			}
		}

		c += size_idx;
	}
}

/*
 * info_obj_root_obj -- print root object
 */
static void
info_obj_root_obj(struct pmem_info *pip, int v,
		struct pmemobjpool *pop)
{
	struct object_store *obj_store = OFF_TO_PTR(pop,
			pop->obj_store_offset);
	struct list_entry *entry = PLIST_OFF_TO_PTR(pop,
			obj_store->root.head.pe_first.off);

	if (entry == NULL) {
		outv(v, "\nNo root object...\n");
	} else {
		struct oob_header *oob = ENTRY_TO_OOB_HDR(entry);
		void *data = ENTRY_TO_DATA(entry);

		outv(v, "\nRoot object:\n");
		outv_field(v, "Offset", "0x%016x", PTR_TO_OFF(pop, data));
		outv_field(v, "Size",
				out_get_size_str(oob->size, pip->args.human));

		obj_object_cb(pip, v, VERBOSE_SILENT, pop, entry, 0);
	}
}

/*
 * info_obj_zones -- print zones and chunks
 */
static void
info_obj_zones_chunks(struct pmem_info *pip, struct pmemobjpool *pop)
{
	if (!outv_check(pip->args.obj.vheap) && !outv_check(pip->args.vstats))
		return;

	struct heap_layout *layout = OFF_TO_PTR(pop, pop->heap_offset);
	size_t maxzone = util_heap_max_zone(pop->heap_size);
	pip->obj.stats.n_zones = maxzone;
	pip->obj.stats.zone_stats = calloc(maxzone,
			sizeof (struct pmem_obj_zone_stats));
	if (!pip->obj.stats.zone_stats)
		err(1, "Cannot allocate memory for zone stats");

	for (size_t i = 0; i < maxzone; i++) {
		struct zone *zone = &layout->zones[i];

		if (util_ranges_contain(&pip->args.obj.zone_ranges, i)) {
			int vvv = pip->args.obj.vheap &&
				(pip->args.obj.vzonehdr ||
				pip->args.obj.vchunkhdr);

			outv_title(vvv, "Zone", "%lu", i);

			if (zone->header.magic == ZONE_HEADER_MAGIC)
				pip->obj.stats.n_zones_used++;

			info_obj_zone_hdr(pip, pip->args.obj.vheap &&
					pip->args.obj.vzonehdr,
					pop, &zone->header);

			out_indent(1);
			info_obj_zone_chunks(pip, pop, zone,
					&pip->obj.stats.zone_stats[i]);
			out_indent(-1);
		}
	}
}

/*
 * info_obj_descriptor -- print pmemobj descriptor
 */
static void
info_obj_descriptor(struct pmem_info *pip, int v,
		struct pmemobjpool *pop)
{
	if (!outv_check(v))
		return;

	outv(v, "\nPMEM OBJ Header:\n");

	outv_hexdump(pip->args.vhdrdump, (uint8_t *)pop + sizeof (pop->hdr),
		sizeof (*pop) - sizeof (pop->hdr), sizeof (pop->hdr), 1);

	/* check if layout is zeroed */
	char *layout = util_check_memory((uint8_t *)pop->layout,
			sizeof (pop->layout), 0) ? pop->layout : "(null)";

	/* address for checksum */
	void *dscp = (void *)((uintptr_t)(&pop->hdr) +
			sizeof (struct pool_hdr));

	outv_field(v, "Layout", layout);
	outv_field(v, "Lanes offset", "0x%lx", pop->lanes_offset);
	outv_field(v, "Number of lanes", "%lu", pop->nlanes);
	outv_field(v, "Object store offset", "0x%lx", pop->obj_store_offset);
	outv_field(v, "Object store size", "%s",
			out_get_size_str(pop->obj_store_size, pip->args.human));
	outv_field(v, "Heap offset", "0x%lx", pop->heap_offset);
	outv_field(v, "Heap size", "%lu", pop->heap_size);
	outv_field(v, "Checksum", "%s", out_get_checksum(dscp, OBJ_DSC_P_SIZE,
				&pop->checksum));

	/* run id with -v option */
	outv_field(v + 1, "Run id", "%lu", pop->run_id);
}

/*
 * info_obj_stats_obj_store -- print object store's statistics
 */
static void
info_obj_stats_obj_store(struct pmem_info *pip, int v,
		struct pmem_obj_stats *stats)
{
	outv_field(v, "Number of objects", "%lu",
			stats->n_total_objects);
	outv_field(v, "Number of bytes", "%s", out_get_size_str(
			stats->n_total_bytes, pip->args.human));

	outv_title(v, "Objects by type");
	out_indent(1);
	struct range *type_curp;
	FOREACH_RANGE(type_curp, &pip->args.obj.object_ranges) {
		for (size_t i = type_curp->first; i <= type_curp->last &&
				i < PMEMOBJ_NUM_OID_TYPES; i++) {
			if (!stats->n_type_objects[i])
				continue;

			double n_objects_perc = 100.0 *
				(double)stats->n_type_objects[i] /
				(double)stats->n_total_objects;
			double n_bytes_perc = 100.0 *
				(double)stats->n_type_bytes[i] /
				(double)stats->n_total_bytes;

			outv_nl(v);
			outv_field(v, "Type number", "%lu", i);
			outv_field(v, "Number of objects", "%lu [%s]",
				stats->n_type_objects[i],
				out_get_percentage(n_objects_perc));
			outv_field(v, "Number of bytes", "%s [%s]",
				out_get_size_str(
					stats->n_type_bytes[i],
					pip->args.human),
				out_get_percentage(n_bytes_perc));
		}
	}
	out_indent(-1);
}

/*
 * info_boj_stats_alloc_classes -- print allocation classes' statistics
 */
static void
info_obj_stats_alloc_classes(struct pmem_info *pip, int v,
		struct pmem_obj_zone_stats *stats)
{
	uint64_t total_bytes = 0;
	uint64_t total_used = 0;

	out_indent(1);
	for (int class = 0; class < MAX_BUCKETS; class++) {
		uint64_t class_size = heap_class_to_size(class);
		double used_perc = 100.0 *
			(double)stats->class_stats[class].n_used /
			(double)stats->class_stats[class].n_units;

		if (!stats->class_stats[class].n_units)
			continue;
		outv_nl(v);
		outv_field(v, "Unit size", "%s", out_get_size_str(
					class_size, pip->args.human));
		outv_field(v, "Units", "%lu",
				stats->class_stats[class].n_units);
		outv_field(v, "Used units", "%lu [%s]",
				stats->class_stats[class].n_used,
				out_get_percentage(used_perc));

		uint64_t bytes = class_size *
			stats->class_stats[class].n_units;
		uint64_t used = class_size *
			stats->class_stats[class].n_used;

		total_bytes += bytes;
		total_used += used;

		double used_bytes_perc = 100.0 * (double)used / (double)bytes;

		outv_field(v, "Bytes", "%s",
				out_get_size_str(bytes, pip->args.human));
		outv_field(v, "Used bytes", "%s [%s]",
				out_get_size_str(used, pip->args.human),
				out_get_percentage(used_bytes_perc));
	}
	out_indent(-1);

	double used_bytes_perc = 100.0 *
		(double)total_used / (double)total_bytes;

	outv_nl(v);
	outv_field(v, "Total bytes", "%s",
			out_get_size_str(total_bytes, pip->args.human));
	outv_field(v, "Total used bytes", "%s [%s]",
			out_get_size_str(total_used, pip->args.human),
			out_get_percentage(used_bytes_perc));
}

/*
 * info_obj_stats_chunks -- print chunks' statistics
 */
static void
info_obj_stats_chunks(struct pmem_info *pip, int v,
		struct pmem_obj_zone_stats *stats)
{
	outv_field(v, "Number of chunks", "%lu", stats->n_chunks);

	out_indent(1);
	for (unsigned type = 0; type < MAX_CHUNK_TYPE; type++) {
		double type_perc = 100.0 *
			(double)stats->n_chunks_type[type] /
			(double)stats->n_chunks;
		if (stats->n_chunks_type[type]) {
			outv_field(v, out_get_chunk_type_str(type),
				"%lu [%s]",
				stats->n_chunks_type[type],
				out_get_percentage(type_perc));
		}
	}
	out_indent(-1);

	outv_nl(v);
	outv_field(v, "Total chunks size", "%s", out_get_size_str(
				stats->size_chunks, pip->args.human));

	out_indent(1);
	for (unsigned type = 0; type < MAX_CHUNK_TYPE; type++) {
		double type_perc = 100.0 *
			(double)stats->size_chunks_type[type] /
			(double)stats->size_chunks;
		if (stats->size_chunks_type[type]) {
			outv_field(v, out_get_chunk_type_str(type),
				"%lu [%s]",
				stats->size_chunks_type[type],
				out_get_percentage(type_perc));
		}

	}
	out_indent(-1);
}

/*
 * info_obj_add_zone_stats -- add stats to total
 */
static void
info_obj_add_zone_stats(struct pmem_obj_zone_stats *total,
		struct pmem_obj_zone_stats *stats)
{
	total->n_chunks += stats->n_chunks;
	total->size_chunks += stats->size_chunks;

	for (int type = 0; type < MAX_CHUNK_TYPE; type++) {
		total->n_chunks_type[type] +=
			stats->n_chunks_type[type];
		total->size_chunks_type[type] +=
			stats->size_chunks_type[type];
	}

	for (int class = 0; class < MAX_BUCKETS; class++) {
		total->class_stats[class].n_units +=
			stats->class_stats[class].n_units;
		total->class_stats[class].n_used +=
			stats->class_stats[class].n_used;
	}
}

/*
 * info_obj_stats_zones -- print zones' statistics
 */
static void
info_obj_stats_zones(struct pmem_info *pip, int v, struct pmem_obj_stats *stats,
		struct pmem_obj_zone_stats *total)
{
	double used_zones_perc = 100.0 * (double)stats->n_zones_used /
		(double)stats->n_zones;

	outv_field(v, "Number of zones", "%lu", stats->n_zones);
	outv_field(v, "Number of used zones", "%lu [%s]", stats->n_zones_used,
			out_get_percentage(used_zones_perc));

	out_indent(1);
	for (uint64_t i = 0; i < stats->n_zones_used; i++) {
		outv_title(v, "Zone", "%lu", i);

		struct pmem_obj_zone_stats *zstats = &stats->zone_stats[i];

		info_obj_stats_chunks(pip, v, zstats);

		outv_title(v, "Zone's allocation classes");
		info_obj_stats_alloc_classes(pip, v, zstats);

		info_obj_add_zone_stats(total, zstats);
	}
	out_indent(-1);
}

/*
 * info_obj_stats -- print statistics
 */
static void
info_obj_stats(struct pmem_info *pip, int v)
{
	if (!outv_check(v))
		return;

	struct pmem_obj_stats *stats = &pip->obj.stats;
	struct pmem_obj_zone_stats total;
	memset(&total, 0, sizeof (total));

	outv_title(v, "Statistics");

	outv_title(v, "Objects");
	info_obj_stats_obj_store(pip, v, stats);

	outv_title(v, "Heap");
	info_obj_stats_zones(pip, v, stats, &total);

	if (stats->n_zones_used > 1) {
		outv_title(v, "Total zone's statistics");
		outv_title(v, "Chunks statistics");
		info_obj_stats_chunks(pip, v, &total);

		outv_title(v, "Allocation classes");
		info_obj_stats_alloc_classes(pip, v, &total);
	}

}

static struct pmem_info *Pip;

static void
info_obj_sa_sigaction(int signum, siginfo_t *info, void *context)
{
	uintptr_t offset = (uintptr_t)info->si_addr - (uintptr_t)Pip->obj.addr;
	outv_err("Invalid offset 0x%lx\n", offset);
	exit(EXIT_FAILURE);
}

static struct sigaction info_obj_sigaction = {
	.sa_sigaction = info_obj_sa_sigaction,
	.sa_flags = SA_SIGINFO
};

/*
 * info_obj -- print information about obj pool type
 */
int
pmempool_info_obj(struct pmem_info *pip)
{
	pip->obj.addr = pool_set_file_map(pip->pfile, 0);
	if (pip->obj.addr == NULL)
		return -1;

	pip->obj.size = pip->pfile->size;

	Pip = pip;
	if (sigaction(SIGSEGV, &info_obj_sigaction, NULL)) {
		perror("sigaction");
		return -1;
	}

	struct pmemobjpool *pop = pip->obj.addr;

	pip->obj.uuid_lo = pmemobj_get_uuid_lo(pop);

	info_obj_descriptor(pip, VERBOSE_DEFAULT, pop);
	info_obj_lanes(pip, pip->args.obj.vlanes, pop);
	info_obj_root_obj(pip, pip->args.obj.vroot, pop);
	info_obj_store(pip, pip->args.obj.vobjects, pop);
	info_obj_heap(pip, pip->args.obj.vheap, pop);
	info_obj_zones_chunks(pip, pop);
	info_obj_stats(pip, pip->args.vstats);

	return 0;
}
