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
 * info_blk.c -- pmempool info command source file for blk pool
 */
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <err.h>
#include <sys/param.h>

#include "common.h"
#include "output.h"
#include "info.h"

/*
 * pmempool_info_get_range -- get blocks/data chunk range
 *
 * Get range based on command line arguments and maximum value.
 * Return value:
 * 0 - range is empty
 * 1 - range is not empty
 */
static int
pmempool_info_get_range(struct pmem_info *pip, struct range *rangep,
		struct range *curp, uint32_t max, uint64_t offset)
{
	/* not using range */
	if (!pip->args.use_range) {
		rangep->first = 0;
		rangep->last = max;

		return 1;
	}

	if (curp->first > offset + max)
		return 0;

	if (curp->first >= offset)
		rangep->first = curp->first - offset;
	else
		rangep->first = 0;

	if (curp->last < offset)
		return 0;

	if (curp->last <= offset + max)
		rangep->last = curp->last - offset;
	else
		rangep->last = max;

	return 1;
}

/*
 * info_blk_skip_block -- get action type for block/data chunk
 *
 * Return value indicating whether processing block/data chunk
 * should be skipped.
 *
 * Return values:
 *  0 - continue processing
 *  1 - skip current block
 */
static int
info_blk_skip_block(struct pmem_info *pip, int is_zero,
		int is_error)
{
	if (pip->args.blk.skip_no_flag && !is_zero && !is_error)
		return 1;

	if (is_zero && pip->args.blk.skip_zeros)
		return 1;

	if (is_error && pip->args.blk.skip_error)
		return 1;

	return 0;
}

/*
 * info_btt_data -- print block data and corresponding flags from map
 */
static int
info_btt_data(struct pmem_info *pip, int v, struct btt_info *infop,
		uint64_t arena_off, uint64_t offset, uint64_t *countp)
{
	if (!outv_check(v))
		return 0;

	int ret = 0;

	size_t mapsize = infop->external_nlba * BTT_MAP_ENTRY_SIZE;
	uint32_t *map = malloc(mapsize);
	if (!map)
		err(1, "Cannot allocate memory for BTT map");

	uint8_t *block_buff = malloc(infop->external_lbasize);
	if (!block_buff)
		err(1, "Cannot allocate memory for pmemblk block buffer");

	/* read btt map area */
	if (pmempool_info_read(pip, (uint8_t *)map, mapsize,
				arena_off + infop->mapoff)) {
		outv_err("wrong BTT Map size or offset\n");
		ret = -1;
		goto error;
	}

	uint64_t i;
	struct range *curp = NULL;
	struct range range;
	FOREACH_RANGE(curp, &pip->args.ranges) {
		if (pmempool_info_get_range(pip, &range, curp,
					infop->external_nlba - 1, offset) == 0)
			continue;
		for (i = range.first; i <= range.last; i++) {
			uint32_t map_entry = le32toh(map[i]);
			int is_init = (map_entry & ~BTT_MAP_ENTRY_LBA_MASK)
				== 0;
			int is_zero = (map_entry & ~BTT_MAP_ENTRY_LBA_MASK)
				== BTT_MAP_ENTRY_ZERO || is_init;
			int is_error = (map_entry & ~BTT_MAP_ENTRY_LBA_MASK)
				== BTT_MAP_ENTRY_ERROR;

			uint64_t blockno = is_init ? i :
					map_entry & BTT_MAP_ENTRY_LBA_MASK;

			if (info_blk_skip_block(pip,
						is_zero, is_error))
				continue;

			/* compute block's data address */
			uint64_t block_off = arena_off + infop->dataoff +
				blockno * infop->internal_lbasize;

			if (pmempool_info_read(pip, block_buff,
					infop->external_lbasize, block_off)) {
				outv_err("cannot read %d block\n", i);
				ret = -1;
				goto error;
			}

			if (*countp == 0)
				outv_title(v, "PMEM BLK blocks data");

			/*
			 * Print block number, offset and flags
			 * from map entry.
			 */
			outv(v, "Block %10d: offset: %s\n",
				offset + i,
				out_get_btt_map_entry(map_entry));

			/* dump block's data */
			outv_hexdump(v, block_buff, infop->external_lbasize,
					block_off, 1);

			*countp = *countp + 1;
		}
	}
error:
	free(map);
	free(block_buff);
	return ret;
}

/*
 * info_btt_map -- print all map entries
 */
static int
info_btt_map(struct pmem_info *pip, int v,
		struct btt_info *infop, uint64_t arena_off,
		uint64_t offset, uint64_t *count)
{
	if (!outv_check(v) && !outv_check(pip->args.vstats))
		return 0;

	int ret = 0;
	size_t mapsize = infop->external_nlba * BTT_MAP_ENTRY_SIZE;

	uint32_t *map = malloc(mapsize);
	if (!map)
		err(1, "Cannot allocate memory for BTT map");

	/* read btt map area */
	if (pmempool_info_read(pip, (uint8_t *)map, mapsize,
				arena_off + infop->mapoff)) {
		outv_err("wrong BTT Map size or offset\n");
		ret = -1;
		goto error;
	}

	uint32_t arena_count = 0;

	uint64_t i;
	struct range *curp = NULL;
	struct range range;
	FOREACH_RANGE(curp, &pip->args.ranges) {
		if (pmempool_info_get_range(pip, &range, curp,
					infop->external_nlba - 1, offset) == 0)
			continue;
		for (i = range.first; i <= range.last; i++) {
			uint32_t entry = le32toh(map[i]);
			int is_zero = (entry & ~BTT_MAP_ENTRY_LBA_MASK) ==
				BTT_MAP_ENTRY_ZERO ||
				(entry & ~BTT_MAP_ENTRY_LBA_MASK) == 0;
			int is_error = (entry & ~BTT_MAP_ENTRY_LBA_MASK) ==
				BTT_MAP_ENTRY_ERROR;

			if (info_blk_skip_block(pip,
					is_zero, is_error) == 0) {
				if (arena_count == 0)
					outv_title(v, "PMEM BLK BTT Map");

				if (is_zero)
					pip->blk.stats.zeros++;
				if (is_error)
					pip->blk.stats.errors++;
				if (!is_zero && !is_error)
					pip->blk.stats.noflag++;

				pip->blk.stats.total++;

				arena_count++;
				(*count)++;

				outv(v, "%010d: %s\n", offset + i,
					out_get_btt_map_entry(entry));
			}
		}
	}
error:
	free(map);
	return ret;
}

/*
 * info_btt_flog_convert -- convert flog entry
 */
static void
info_btt_flog_convert(struct btt_flog *flogp)
{
	flogp->lba = le32toh(flogp->lba);
	flogp->old_map = le32toh(flogp->old_map);
	flogp->new_map = le32toh(flogp->new_map);
	flogp->seq = le32toh(flogp->seq);
}

/*
 * info_btt_flog -- print all flog entries
 */
static int
info_btt_flog(struct pmem_info *pip, int v,
		struct btt_info *infop, uint64_t arena_off)
{
	if (!outv_check(v))
		return 0;

	int ret = 0;
	struct btt_flog *flogp = NULL;
	struct btt_flog *flogpp = NULL;
	uint64_t flog_size = infop->nfree *
		roundup(2 * sizeof (struct btt_flog), BTT_FLOG_PAIR_ALIGN);
	flog_size = roundup(flog_size, BTT_ALIGNMENT);
	uint8_t *buff = malloc(flog_size);
	if (!buff)
		err(1, "Cannot allocate memory for FLOG entries");

	if (pmempool_info_read(pip, buff, flog_size,
				arena_off + infop->flogoff)) {
		outv_err("cannot read BTT FLOG");
		ret = -1;
		goto error;
	}

	outv_title(v, "PMEM BLK BTT FLOG");

	uint8_t *ptr = buff;
	uint32_t i;
	for (i = 0; i < infop->nfree; i++) {
		flogp = (struct btt_flog *)ptr;
		flogpp = flogp + 1;

		info_btt_flog_convert(flogp);
		info_btt_flog_convert(flogpp);

		outv(v, "%010d:\n", i);
		outv_field(v, "LBA", "0x%08x", flogp->lba);
		outv_field(v, "Old map", "0x%08x: %s", flogp->old_map,
			out_get_btt_map_entry(flogp->old_map));
		outv_field(v, "New map", "0x%08x: %s", flogp->new_map,
			out_get_btt_map_entry(flogp->new_map));
		outv_field(v, "Seq", "0x%x", flogp->seq);

		outv_field(v, "LBA'", "0x%08x", flogpp->lba);
		outv_field(v, "Old map'", "0x%08x: %s", flogpp->old_map,
			out_get_btt_map_entry(flogpp->old_map));
		outv_field(v, "New map'", "0x%08x: %s", flogpp->new_map,
			out_get_btt_map_entry(flogpp->new_map));
		outv_field(v, "Seq'", "0x%x", flogpp->seq);

		ptr += BTT_FLOG_PAIR_ALIGN;
	}
error:
	free(buff);
	return ret;
}

/*
 * info_btt_stats -- print btt related statistics
 */
static void
info_btt_stats(struct pmem_info *pip, int v)
{
	if (pip->blk.stats.total > 0) {
		outv_title(v, "PMEM BLK Statistics");
		double perc_zeros = (double)pip->blk.stats.zeros /
			(double)pip->blk.stats.total * 100.0;
		double perc_errors = (double)pip->blk.stats.errors /
			(double)pip->blk.stats.total * 100.0;
		double perc_noflag = (double)pip->blk.stats.noflag /
			(double)pip->blk.stats.total * 100.0;

		outv_field(v, "Total blocks", "%u", pip->blk.stats.total);
		outv_field(v, "Zeroed blocks", "%u [%s]", pip->blk.stats.zeros,
				out_get_percentage(perc_zeros));
		outv_field(v, "Error blocks", "%u [%s]", pip->blk.stats.errors,
				out_get_percentage(perc_errors));
		outv_field(v, "Blocks without flag", "%u [%s]",
				pip->blk.stats.noflag,
				out_get_percentage(perc_noflag));
	}
}

/*
 * info_btt_info -- print btt_info structure fields
 */
static int
info_btt_info(struct pmem_info *pip, int v, struct btt_info *infop)
{
	outv_field(v, "Signature", "%.*s", BTTINFO_SIG_LEN, infop->sig);

	outv_field(v, "UUID of container", "%s",
			out_get_uuid_str(infop->parent_uuid));

	outv_field(v, "Flags", "0x%x", infop->flags);
	outv_field(v, "Major", "%d", infop->major);
	outv_field(v, "Minor", "%d", infop->minor);
	outv_field(v, "External LBA size", "%s",
			out_get_size_str(infop->external_lbasize,
				pip->args.human));
	outv_field(v, "External LBA count", "%u", infop->external_nlba);
	outv_field(v, "Internal LBA size", "%s",
			out_get_size_str(infop->internal_lbasize,
				pip->args.human));
	outv_field(v, "Internal LBA count", "%u", infop->internal_nlba);
	outv_field(v, "Free blocks", "%u", infop->nfree);
	outv_field(v, "Info block size", "%s",
		out_get_size_str(infop->infosize, pip->args.human));
	outv_field(v, "Next arena offset", "0x%lx", infop->nextoff);
	outv_field(v, "Arena data offset", "0x%lx", infop->dataoff);
	outv_field(v, "Area map offset", "0x%lx", infop->mapoff);
	outv_field(v, "Area flog offset", "0x%lx", infop->flogoff);
	outv_field(v, "Info block backup offset", "0x%lx", infop->infooff);
	outv_field(v, "Checksum", "%s", out_get_checksum(infop,
				sizeof (*infop), &infop->checksum));

	return 0;
}

/*
 * info_btt_layout -- print information about BTT layout
 */
static int
info_btt_layout(struct pmem_info *pip, off_t btt_off)
{
	int ret = 0;

	if (btt_off <= 0) {
		outv_err("wrong BTT layout offset\n");
		return -1;
	}

	struct btt_info *infop = NULL;

	infop = malloc(sizeof (struct btt_info));
	if (!infop)
		err(1, "Cannot allocate memory for BTT Info structure");

	int narena = 0;
	uint64_t cur_lba = 0;
	uint64_t count_data = 0;
	uint64_t count_map = 0;
	uint64_t offset = (uint64_t)btt_off;
	uint64_t nextoff = 0;

	do {
		/* read btt info area */
		if (pmempool_info_read(pip, infop, sizeof (*infop), offset)) {
			ret = -1;
			outv_err("cannot read BTT Info header\n");
			goto err;
		}

		if (util_check_memory((uint8_t *)infop,
					sizeof (*infop), 0) == 0) {
			outv(1, "\n<No BTT layout>\n");
			break;
		}

		outv(1, "\n[ARENA %d]", narena);
		outv_title(1, "PMEM BLK BTT Info Header");
		outv_hexdump(pip->args.vhdrdump, infop,
				sizeof (*infop), offset, 1);

		util_convert2h_btt_info(infop);

		nextoff = infop->nextoff;

		/* print btt info fields */
		if (info_btt_info(pip, 1, infop)) {
			ret = -1;
			goto err;
		}

		/* dump blocks data */
		if (info_btt_data(pip, pip->args.vdata,
					infop, offset, cur_lba, &count_data)) {
			ret = -1;
			goto err;
		}

		/* print btt map entries and get statistics */
		if (info_btt_map(pip, pip->args.blk.vmap, infop,
					offset, cur_lba, &count_map)) {
			ret = -1;
			goto err;
		}

		/* print flog entries */
		if (info_btt_flog(pip, pip->args.blk.vflog, infop,
					offset)) {
			ret = -1;
			goto err;
		}

		/* increment LBA's counter before reading info backup */
		cur_lba += infop->external_nlba;

		/* read btt info backup area */
		if (pmempool_info_read(pip, infop, sizeof (*infop),
			offset + infop->infooff)) {
			outv_err("wrong BTT Info Backup size or offset\n");
			ret = -1;
			goto err;
		}

		outv_title(pip->args.blk.vbackup,
				"PMEM BLK BTT Info Header Backup");
		if (outv_check(pip->args.blk.vbackup))
			outv_hexdump(pip->args.vhdrdump, infop,
				sizeof (*infop),
				offset + infop->infooff, 1);

		util_convert2h_btt_info(infop);
		info_btt_info(pip, pip->args.blk.vbackup, infop);

		offset += nextoff;
		narena++;

	} while (nextoff > 0);

	info_btt_stats(pip, pip->args.vstats);

err:
	if (infop)
		free(infop);

	return ret;
}

/*
 * info_blk_descriptor -- print pmemblk descriptor
 */
static void
info_blk_descriptor(struct pmem_info *pip, int v, struct pmemblk *pbp)
{
	outv_title(v, "PMEM BLK Header");
	/* dump pmemblk header without pool_hdr */
	outv_hexdump(pip->args.vhdrdump, (uint8_t *)pbp + sizeof (pbp->hdr),
		sizeof (*pbp) - sizeof (pbp->hdr), sizeof (pbp->hdr), 1);
	outv_field(v, "Block size", "%s",
			out_get_size_str(pbp->bsize, pip->args.human));
	outv_field(v, "Is zeroed", pbp->is_zeroed ? "true" : "false");
}

/*
 * pmempool_info_blk -- print information about block type pool
 */
int
pmempool_info_blk(struct pmem_info *pip)
{
	int ret;
	struct pmemblk *pbp = malloc(sizeof (struct pmemblk));
	if (!pbp)
		err(1, "Cannot allocate memory for pmemblk structure");

	if (pmempool_info_read(pip, pbp, sizeof (struct pmemblk), 0)) {
		outv_err("cannot read pmemblk header\n");
		free(pbp);
		return -1;
	}

	info_blk_descriptor(pip, VERBOSE_DEFAULT, pbp);

	ssize_t btt_off = (char *)pbp->data - (char *)pbp->addr;
	ret = info_btt_layout(pip, btt_off);

	free(pbp);

	return ret;
}
