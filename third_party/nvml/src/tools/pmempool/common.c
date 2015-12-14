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
 * common.c -- definitions of common functions
 */

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <err.h>
#include <sys/param.h>
#include <sys/mman.h>
#include <ctype.h>
#include <assert.h>
#include <getopt.h>
#include <unistd.h>

#include "common.h"
#include "output.h"
#include "libpmemblk.h"
#include "libpmemlog.h"
#include "libpmemobj.h"

#define	REQ_BUFF_SIZE	2048U

typedef const char *(*enum_to_str_fn)(int);

/*
 * pmem_pool_type_parse_hdr -- return pool type based on pool header data
 */
pmem_pool_type_t
pmem_pool_type_parse_hdr(const struct pool_hdr *hdrp)
{
	if (memcmp(hdrp->signature, LOG_HDR_SIG, POOL_HDR_SIG_LEN) == 0)
		return PMEM_POOL_TYPE_LOG;
	else if (memcmp(hdrp->signature, BLK_HDR_SIG, POOL_HDR_SIG_LEN) == 0)
		return PMEM_POOL_TYPE_BLK;
	else if (memcmp(hdrp->signature, OBJ_HDR_SIG, POOL_HDR_SIG_LEN) == 0)
		return PMEM_POOL_TYPE_OBJ;
	else
		return PMEM_POOL_TYPE_UNKNOWN;
}

/*
 * pmem_pool_type_parse_str -- returns pool type from command line arg
 */
pmem_pool_type_t
pmem_pool_type_parse_str(const char *str)
{
	if (strcmp(str, "blk") == 0) {
		return PMEM_POOL_TYPE_BLK;
	} else if (strcmp(str, "log") == 0) {
		return PMEM_POOL_TYPE_LOG;
	} else if (strcmp(str, "obj") == 0) {
		return PMEM_POOL_TYPE_OBJ;
	} else {
		return PMEM_POOL_TYPE_UNKNOWN;
	}
}

/*
 * pmem_pool_check_pool_set -- returns 0 for poolset file
 */
int
pmem_pool_check_pool_set(const char *fname)
{
	int fd = util_file_open(fname, NULL, 0, O_RDONLY);
	if (fd < 0)
		return -1;

	int ret = 0;
	char poolset[POOLSET_HDR_SIG_LEN];
	if (read(fd, poolset, sizeof (poolset)) != sizeof (poolset)) {
		ret = -1;
		goto out;
	}

	if (strncmp(poolset, POOLSET_HDR_SIG, POOLSET_HDR_SIG_LEN))
		ret = 1;
out:
	close(fd);
	return ret;
}

/*
 * util_validate_checksum -- validate checksum and return valid one
 */
int
util_validate_checksum(void *addr, size_t len, uint64_t *csum)
{
	/* validate checksum */
	int csum_valid = util_checksum(addr, len, csum, 0);
	/* get valid one */
	if (!csum_valid)
		util_checksum(addr, len, csum, 1);
	return csum_valid;
}

/*
 * util_pool_hdr_valid -- return 1 if pool header is valid
 */
int
util_pool_hdr_valid(struct pool_hdr *hdrp)
{
	return util_check_memory((void *)hdrp, sizeof (*hdrp), 0) &&
		util_checksum(hdrp, sizeof (*hdrp), &hdrp->checksum, 0);
}


/*
 * util_parse_size -- parse size from string
 */
int
util_parse_size(const char *str, uint64_t *sizep)
{
	uint64_t size = 0;
	int shift = 0;
	char unit[3] = {0};
	int ret = sscanf(str, "%lu%2s", &size, unit);
	if (ret <= 0)
		return -1;
	if (ret == 2) {
		if ((unit[1] != '\0' && unit[1] != 'B') ||
			unit[2] != '\0')
			return -1;
		switch (unit[0]) {
		case 'K':
			shift = 10;
			break;
		case 'M':
			shift = 20;
			break;
		case 'G':
			shift = 30;
			break;
		case 'T':
			shift = 40;
			break;
		case 'P':
			shift = 50;
			break;
		default:
			return -1;
		}
	}

	if (sizep)
		*sizep = size << shift;

	return 0;
}

/*
 * util_parse_mode -- parse file mode from octal string
 */
int
util_parse_mode(const char *str, mode_t *mode)
{
	mode_t m = 0;
	int digits = 0;

	/* skip leading zeros */
	while (*str == '0')
		str++;

	/* parse at most 3 octal digits */
	while (digits < 3 && *str != '\0') {
		if (*str < '0' || *str > '7')
			return -1;
		m = (m << 3) | (unsigned)(*str - '0');
		digits++;
		str++;
	}

	/* more than 3 octal digits */
	if (digits == 3 && *str != '\0')
		return -1;

	if (mode)
		*mode = m;

	return 0;
}

static void
util_range_limit(struct range *rangep, struct range limit)
{
	if (rangep->first < limit.first)
		rangep->first = limit.first;
	if (rangep->last > limit.last)
		rangep->last = limit.last;
}

/*
 * util_parse_range_from_to -- parse range string as interval
 */
static int
util_parse_range_from_to(char *str, struct range *rangep, struct range entire)
{
	char *str1 = NULL;
	char sep;
	char *str2 = NULL;

	int ret = 0;
	if (sscanf(str, "%m[^-]%c%m[^-]", &str1, &sep, &str2) == 3 &&
			sep == '-' &&
			strlen(str) == (strlen(str1) + 1 + strlen(str2))) {
		if (util_parse_size(str1, &rangep->first) != 0)
			ret = -1;
		else if (util_parse_size(str2, &rangep->last) != 0)
			ret = -1;

		if (rangep->first > rangep->last) {
			uint64_t tmp = rangep->first;
			rangep->first = rangep->last;
			rangep->last = tmp;
		}

		util_range_limit(rangep, entire);
	} else {
		ret = -1;
	}

	if (str1)
		free(str1);
	if (str2)
		free(str2);

	return ret;
}

/*
 * util_parse_range_from -- parse range string as interval from specified number
 */
static int
util_parse_range_from(char *str, struct range *rangep, struct range entire)
{
	char *str1 = NULL;
	char sep;

	int ret = 0;
	if (sscanf(str, "%m[^-]%c", &str1, &sep) == 2 &&
			sep == '-' &&
			strlen(str) == (strlen(str1) + 1)) {
		if (util_parse_size(str1, &rangep->first) == 0) {
			rangep->last = entire.last;
			util_range_limit(rangep, entire);
		} else {
			ret = -1;
		}
	} else {
		ret = -1;
	}

	if (str1)
		free(str1);

	return ret;
}

/*
 * util_parse_range_to -- parse range string as interval to specified number
 */
static int
util_parse_range_to(char *str, struct range *rangep, struct range entire)
{
	char *str1 = NULL;
	char sep;

	int ret = 0;
	if (sscanf(str, "%c%m[^-]", &sep, &str1) == 2 &&
			sep == '-' &&
			strlen(str) == (1 + strlen(str1))) {
		if (util_parse_size(str1, &rangep->last) == 0) {
			rangep->first = entire.first;
			util_range_limit(rangep, entire);
		} else {
			ret = -1;
		}
	} else {
		ret = -1;
	}

	if (str1)
		free(str1);

	return ret;
}

/*
 * util_parse_range_number -- parse range string as a single number
 */
static int
util_parse_range_number(char *str, struct range *rangep, struct range entire)
{
	if (util_parse_size(str, &rangep->first) != 0)
		return -1;
	rangep->last = rangep->first;
	if (rangep->first > entire.last ||
	    rangep->last < entire.first)
		return -1;
	util_range_limit(rangep, entire);
	return 0;
}

/*
 * util_parse_range -- parse single range string
 */
static int
util_parse_range(char *str, struct range *rangep, struct range entire)
{
	if (util_parse_range_from_to(str, rangep, entire) == 0)
		return 0;
	if (util_parse_range_from(str, rangep, entire) == 0)
		return 0;
	if (util_parse_range_to(str, rangep, entire) == 0)
		return 0;
	if (util_parse_range_number(str, rangep, entire) == 0)
		return 0;
	return -1;
}

/*
 * util_ranges_overlap -- return 1 if two ranges are overlapped
 */
static int
util_ranges_overlap(struct range *rangep1, struct range *rangep2)
{
	if (rangep1->last + 1 < rangep2->first ||
	    rangep2->last + 1 < rangep1->first)
		return 0;
	else
		return 1;
}

/*
 * util_ranges_add -- create and add range
 */
int
util_ranges_add(struct ranges *rangesp, struct range range)
{
	struct range *rangep = malloc(sizeof (struct range));
	if (!rangep)
		err(1, "Cannot allocate memory for range\n");
	memcpy(rangep, &range, sizeof (*rangep));

	struct range *curp, *next;
	uint64_t first = rangep->first;
	uint64_t last = rangep->last;

	curp = LIST_FIRST(&rangesp->head);
	while (curp) {
		next = LIST_NEXT(curp, next);
		if (util_ranges_overlap(curp, rangep)) {
			LIST_REMOVE(curp, next);
			if (curp->first < first)
				first = curp->first;
			if (curp->last > last)
				last = curp->last;
			free(curp);
		}
		curp = next;
	}

	rangep->first = first;
	rangep->last = last;

	LIST_FOREACH(curp, &rangesp->head, next) {
		if (curp->first < rangep->first) {
			LIST_INSERT_AFTER(curp, rangep, next);
			return 0;
		}
	}

	LIST_INSERT_HEAD(&rangesp->head, rangep, next);

	return 0;
}

/*
 * util_ranges_contain -- return 1 if ranges contain the number n
 */
int
util_ranges_contain(const struct ranges *rangesp, uint64_t n)
{
	struct range *curp  = NULL;
	LIST_FOREACH(curp, &rangesp->head, next) {
		if (curp->first <= n && n <= curp->last)
			return 1;
	}

	return 0;
}

/*
 * util_ranges_empty -- return 1 if ranges are empty
 */
int
util_ranges_empty(const struct ranges *rangesp)
{
	return LIST_EMPTY(&rangesp->head);
}

/*
 * util_ranges_clear -- clear list of ranges
 */
void
util_ranges_clear(struct ranges *rangesp)
{
	while (!LIST_EMPTY(&rangesp->head)) {
		struct range *rangep = LIST_FIRST(&rangesp->head);
		LIST_REMOVE(rangep, next);
		free(rangep);
	}
}

/*
 * util_parse_ranges -- parser ranges from string
 *
 * The valid formats of range are:
 * - 'n-m' -- from n to m
 * - '-m'  -- from minimum passed in entirep->first to m
 * - 'n-'  -- from n to maximum passed in entirep->last
 * - 'n'   -- n'th byte/block
 * Multiple ranges may be separated by comma:
 * 'n1-m1,n2-,-m3,n4'
 */
int
util_parse_ranges(const char *ptr, struct ranges *rangesp, struct range entire)
{
	if (ptr == NULL)
		return util_ranges_add(rangesp, entire);

	char *dup = strdup(ptr);
	if (!dup)
		err(1, "Cannot allocate memory for ranges");
	char *str = dup;
	int ret = 0;
	char *next = str;
	do {
		str = next;
		next = strchr(str, ',');
		if (next != NULL) {
			*next = '\0';
			next++;
		}
		struct range range;
		if (util_parse_range(str, &range, entire)) {
			ret = -1;
			goto out;
		} else if (util_ranges_add(rangesp, range)) {
			ret = -1;
			goto out;
		}
	} while (next != NULL);
out:
	free(dup);
	return ret;
}

/*
 * pmem_pool_get_min_size -- return minimum size of pool for specified type
 */
uint64_t
pmem_pool_get_min_size(pmem_pool_type_t type)
{
	switch (type) {
	case PMEM_POOL_TYPE_LOG:
		return PMEMLOG_MIN_POOL;
	case PMEM_POOL_TYPE_BLK:
		return PMEMBLK_MIN_POOL;
	case PMEM_POOL_TYPE_OBJ:
		return PMEMOBJ_MIN_POOL;
	default:
		break;
	}

	return 0;
}

/*
 * pmem_pool_get_hdr_size -- return size of header for specified type
 */
size_t
pmem_pool_get_hdr_size(pmem_pool_type_t type)
{
	switch (type) {
	case PMEM_POOL_TYPE_LOG:
		return sizeof (struct pmemlog);
	case PMEM_POOL_TYPE_BLK:
		return sizeof (struct pmemblk);
	case PMEM_POOL_TYPE_OBJ:
		return sizeof (struct pmemobjpool);
	default:
		break;
	}

	return 0;
}

/*
 * util_poolset_map -- map poolset
 */
int
util_poolset_map(const char *fname, struct pool_set **poolset, int rdonly)
{
	if (pmem_pool_check_pool_set(fname) != 0) {
		return util_pool_open_nocheck(poolset, fname, rdonly,
					DEFAULT_HDR_SIZE);
	}

	int fd = util_file_open(fname, NULL, 0, O_RDONLY);
	if (fd < 0)
		return -1;

	int ret = 0;

	struct pool_set *set = NULL;

	/* parse poolset file */
	if (util_poolset_parse(fname, fd, &set)) {
		outv_err("parsing poolset file failed\n");
		ret = -1;
		goto err_close;
	}

	/* open the first part set file to read the pool header values */
	int fdp = util_file_open(set->replica[0]->part[0].path,
			NULL, 0, O_RDONLY);
	if (fdp < 0) {
		outv_err("cannot open poolset part file\n");
		ret = -1;
		goto err_pool_set;
	}

	struct pool_hdr hdr;
	/* read the pool header from first pool set file */
	if (pread(fdp, &hdr, sizeof (hdr), 0)
			!= sizeof (hdr)) {
		outv_err("cannot read pool header from poolset\n");
		ret = -1;
		goto err_close_part;
	}

	close(fdp);
	util_poolset_free(set);
	close(fd);

	util_convert2h_pool_hdr(&hdr);

	/* parse pool type from first pool set file */
	pmem_pool_type_t type = pmem_pool_type_parse_hdr(&hdr);
	if (type == PMEM_POOL_TYPE_UNKNOWN) {
		outv_err("cannot determine pool type from poolset\n");
		return -1;
	}

	/* get minimum size based on pool type for util_pool_open */
	size_t minsize = pmem_pool_get_min_size(type);

	size_t hdrsize = pmem_pool_get_hdr_size(type);

	/*
	 * Open the poolset, the values passed to util_pool_open are read
	 * from the first poolset file, these values are then compared with
	 * the values from all headers of poolset files.
	 */
	if (util_pool_open(poolset, fname, rdonly, minsize,
			roundup(hdrsize, Pagesize),
			hdr.signature, hdr.major,
			hdr.compat_features,
			hdr.incompat_features,
			hdr.ro_compat_features)) {
		outv_err("openning poolset failed\n");
		return -1;
	}

	return 0;
err_close_part:
	close(fdp);
err_pool_set:
	util_poolset_free(set);
err_close:
	close(fd);

	return ret;
}

/*
 * pmem_pool_parse_params -- parse pool type, file size and block size
 */
int
pmem_pool_parse_params(const char *fname, struct pmem_pool_params *paramsp,
		int check)
{
	struct stat stat_buf;
	paramsp->type = PMEM_POOL_TYPE_UNKNOWN;

	paramsp->is_poolset = pmem_pool_check_pool_set(fname) == 0;
	int fd = util_file_open(fname, NULL, 0, O_RDONLY);
	if (fd < 0)
		return -1;

	int ret = 0;

	/* get file size and mode */
	if (fstat(fd, &stat_buf)) {
		ret = -1;
		goto out_close;
	}

	assert(stat_buf.st_size >= 0);
	paramsp->size = (uint64_t)stat_buf.st_size;
	paramsp->mode = stat_buf.st_mode;

	void *addr = NULL;
	struct pool_set *set = NULL;
	if (paramsp->is_poolset) {
		/* close the file */
		close(fd);
		fd = -1;

		if (check) {
			if (util_poolset_map(fname, &set, 1))
				return -1;
		} else {
			if (util_pool_open_nocheck(&set, fname, 1,
						DEFAULT_HDR_SIZE))
				return -1;
		}

		paramsp->size = set->poolsize;
		addr = set->replica[0]->part[0].addr;
	} else {
		addr = mmap(NULL, (uint64_t)stat_buf.st_size,
				PROT_READ, MAP_PRIVATE, fd, 0);
		if (addr == MAP_FAILED) {
			ret = -1;
			goto out_close;
		}
	}

	struct pool_hdr hdr;
	memcpy(&hdr, addr, sizeof (hdr));

	util_convert2h_pool_hdr(&hdr);

	memcpy(paramsp->signature, hdr.signature, sizeof (paramsp->signature));

	/*
	 * Check if file is a part of pool set by comparing
	 * the UUID with the next part UUID. If it is the same
	 * it means the pool consist of a single file.
	 */
	paramsp->is_part = !paramsp->is_poolset &&
		(memcmp(hdr.uuid, hdr.next_part_uuid, POOL_HDR_UUID_LEN) ||
		memcmp(hdr.uuid, hdr.prev_part_uuid, POOL_HDR_UUID_LEN));

	paramsp->type = pmem_pool_type_parse_hdr(&hdr);

	if (paramsp->type == PMEM_POOL_TYPE_BLK) {
		struct pmemblk pbp;
		memcpy(&pbp, addr, sizeof (pbp));
		paramsp->blk.bsize = le32toh(pbp.bsize);
	} else if (paramsp->type == PMEM_POOL_TYPE_OBJ) {
		struct pmemobjpool pop;
		memcpy(&pop, addr, sizeof (pop));
		memcpy(paramsp->obj.layout, pop.layout, PMEMOBJ_MAX_LAYOUT);
	}

	if (paramsp->is_poolset)
		util_poolset_close(set, 0);
	else
		munmap(addr, (uint64_t)stat_buf.st_size);
out_close:
	if (fd >= 0)
		(void) close(fd);
	return ret;
}

/*
 * pmem_default_pool_hdr -- return default pool header values
 */
void
pmem_default_pool_hdr(pmem_pool_type_t type, struct pool_hdr *hdrp)
{
	memset(hdrp, 0, sizeof (*hdrp));
	const char *sig = out_get_pool_signature(type);
	assert(sig);

	memcpy(hdrp->signature, sig, POOL_HDR_SIG_LEN);

	switch (type) {
	case PMEM_POOL_TYPE_LOG:
		hdrp->major = LOG_FORMAT_MAJOR;
		hdrp->compat_features = LOG_FORMAT_COMPAT;
		hdrp->incompat_features = LOG_FORMAT_INCOMPAT;
		hdrp->ro_compat_features = LOG_FORMAT_RO_COMPAT;
		break;
	case PMEM_POOL_TYPE_BLK:
		hdrp->major = BLK_FORMAT_MAJOR;
		hdrp->compat_features = BLK_FORMAT_COMPAT;
		hdrp->incompat_features = BLK_FORMAT_INCOMPAT;
		hdrp->ro_compat_features = BLK_FORMAT_RO_COMPAT;
		break;
	case PMEM_POOL_TYPE_OBJ:
		hdrp->major = OBJ_FORMAT_MAJOR;
		hdrp->compat_features = OBJ_FORMAT_COMPAT;
		hdrp->incompat_features = OBJ_FORMAT_INCOMPAT;
		hdrp->ro_compat_features = OBJ_FORMAT_RO_COMPAT;
		break;
	default:
		break;
	}
}

/*
 * util_pool_hdr_convert -- convert pool header to host byte order
 */
void
util_convert2h_pool_hdr(struct pool_hdr *hdrp)
{
	hdrp->compat_features = le32toh(hdrp->compat_features);
	hdrp->incompat_features = le32toh(hdrp->incompat_features);
	hdrp->ro_compat_features = le32toh(hdrp->ro_compat_features);
	hdrp->arch_flags.alignment_desc =
		le64toh(hdrp->arch_flags.alignment_desc);
	hdrp->arch_flags.e_machine = le16toh(hdrp->arch_flags.e_machine);
	hdrp->crtime = le64toh(hdrp->crtime);
	hdrp->checksum = le64toh(hdrp->checksum);
}

/*
 * util_pool_hdr_convert -- convert pool header to LE byte order
 */
void
util_convert2le_pool_hdr(struct pool_hdr *hdrp)
{
	hdrp->compat_features = htole32(hdrp->compat_features);
	hdrp->incompat_features = htole32(hdrp->incompat_features);
	hdrp->ro_compat_features = htole32(hdrp->ro_compat_features);
	hdrp->arch_flags.alignment_desc =
		htole64(hdrp->arch_flags.alignment_desc);
	hdrp->arch_flags.e_machine = htole16(hdrp->arch_flags.e_machine);
	hdrp->crtime = htole64(hdrp->crtime);
	hdrp->checksum = htole64(hdrp->checksum);
}

/*
 * util_convert_btt_info -- convert btt_info header to host byte order
 */
void
util_convert2h_btt_info(struct btt_info *infop)
{
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
	infop->checksum = le64toh(infop->checksum);
}

/*
 * util_convert_btt_info -- convert btt_info header to LE byte order
 */
void
util_convert2le_btt_info(struct btt_info *infop)
{
	infop->flags = htole64(infop->flags);
	infop->minor = htole16(infop->minor);
	infop->external_lbasize = htole32(infop->external_lbasize);
	infop->external_nlba = htole32(infop->external_nlba);
	infop->internal_lbasize = htole32(infop->internal_lbasize);
	infop->internal_nlba = htole32(infop->internal_nlba);
	infop->nfree = htole32(infop->nfree);
	infop->infosize = htole32(infop->infosize);
	infop->nextoff = htole64(infop->nextoff);
	infop->dataoff = htole64(infop->dataoff);
	infop->mapoff = htole64(infop->mapoff);
	infop->flogoff = htole64(infop->flogoff);
	infop->infooff = htole64(infop->infooff);
	infop->checksum = htole64(infop->checksum);
}

/*
 * util_convert2h_btt_flog -- convert btt_flog to host byte order
 */
void
util_convert2h_btt_flog(struct btt_flog *flogp)
{
	flogp->lba = le32toh(flogp->lba);
	flogp->old_map = le32toh(flogp->old_map);
	flogp->new_map = le32toh(flogp->new_map);
	flogp->seq = le32toh(flogp->seq);
}

/*
 * util_convert2le_btt_flog -- convert btt_flog to LE byte order
 */
void
util_convert2le_btt_flog(struct btt_flog *flogp)
{
	flogp->lba = htole32(flogp->lba);
	flogp->old_map = htole32(flogp->old_map);
	flogp->new_map = htole32(flogp->new_map);
	flogp->seq = htole32(flogp->seq);
}

/*
 * util_convert2h_pmemlog -- convert pmemlog structure to host byte order
 */
void
util_convert2h_pmemlog(struct pmemlog *plp)
{
	plp->start_offset = le64toh(plp->start_offset);
	plp->end_offset = le64toh(plp->end_offset);
	plp->write_offset = le64toh(plp->write_offset);
}

/*
 * util_convert2le_pmemlog -- convert pmemlog structure to LE byte order
 */
void
util_convert2le_pmemlog(struct pmemlog *plp)
{
	plp->start_offset = htole64(plp->start_offset);
	plp->end_offset = htole64(plp->end_offset);
	plp->write_offset = htole64(plp->write_offset);
}

/*
 * util_check_memory -- check if memory contains single value
 */
int
util_check_memory(const uint8_t *buff, size_t len, uint8_t val)
{
	size_t i;
	for (i = 0; i < len; i++) {
		if (buff[i] != val)
			return -1;
	}

	return 0;
}

/*
 * util_get_max_bsize -- return maximum size of block for given file size
 */
uint32_t
util_get_max_bsize(uint64_t fsize)
{
	if (fsize == 0)
		return 0;

	/* default nfree */
	uint32_t nfree = BTT_DEFAULT_NFREE;

	/* number of blocks must be at least 2 * nfree */
	uint32_t internal_nlba = 2 * nfree;

	/* compute flog size */
	uint32_t flog_size = nfree *
		(uint32_t)roundup(2 * sizeof (struct btt_flog),
				BTT_FLOG_PAIR_ALIGN);
	flog_size = (uint32_t)roundup(flog_size, BTT_ALIGNMENT);

	/* compute arena size from file size */
	uint64_t arena_size = fsize;
	/* without pmemblk structure */
	arena_size -= sizeof (struct pmemblk);
	if (arena_size > BTT_MAX_ARENA) {
		arena_size = BTT_MAX_ARENA;
	}
	/* without BTT Info header and backup */
	arena_size -= 2 * sizeof (struct btt_info);
	/* without BTT FLOG size */
	arena_size -= flog_size;

	/* compute maximum internal LBA size */
	uint64_t internal_lbasize = (arena_size - BTT_ALIGNMENT) /
			internal_nlba - BTT_MAP_ENTRY_SIZE;
	assert(internal_lbasize <= UINT32_MAX);

	if (internal_lbasize < BTT_MIN_LBA_SIZE)
		internal_lbasize = BTT_MIN_LBA_SIZE;

	internal_lbasize =
		roundup(internal_lbasize, BTT_INTERNAL_LBA_ALIGNMENT)
			- BTT_INTERNAL_LBA_ALIGNMENT;

	return (uint32_t)internal_lbasize;
}

/*
 * util_check_bsize -- check if block size is valid for given file size
 */
int
util_check_bsize(uint32_t bsize, uint64_t fsize)
{
	uint32_t max_bsize = util_get_max_bsize(fsize);
	return !(bsize < max_bsize);
}

char
ask(char op, char *answers, char def_ans, const char *fmt, va_list ap)
{
	char ans = '\0';
	if (op != '?')
		return op;
	int is_tty = isatty(fileno(stdin));
	do {
		vprintf(fmt, ap);
		size_t len = strlen(answers);
		size_t i;
		char def_anslo = (char)tolower(def_ans);
		printf(" [");
		for (i = 0; i < len; i++) {
			char anslo = (char)tolower(answers[i]);
			printf("%c", anslo == def_anslo ?
					toupper(anslo) : anslo);
			if (i != len - 1)
				printf("/");
		}
		printf("] ");
		int c = getchar();
		if (c == EOF)
			ans = def_anslo;
		else
			ans = (char)tolower(c);
		if (ans != '\n')
			(void) getchar();
	} while (ans != '\n' && strchr(answers, ans) == NULL);

	char ret = ans == '\n' ? def_ans : ans;
	if (!is_tty)
		printf("%c\n", ret);
	return ret;
}

char
ask_yn(char op, char def_ans, const char *fmt, va_list ap)
{
	char ret = ask(op, "yn", def_ans, fmt, ap);
	return ret;
}

char
ask_Yn(char op, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	char ret = ask_yn(op, 'y', fmt, ap);
	va_end(ap);
	return ret;
}

char
ask_yN(char op, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	char ret = ask_yn(op, 'n', fmt, ap);
	va_end(ap);
	return ret;
}

/*
 * util_parse_enum -- parse single enum and store to bitmap
 */
static int
util_parse_enum(const char *str, int first, int max, uint64_t *bitmap,
		enum_to_str_fn enum_to_str)
{
	for (int i = first; i < max; i++) {
		if (strcmp(str, enum_to_str(i)) == 0) {
			*bitmap |= (uint64_t)1<<i;
			return 0;
		}
	}

	return -1;
}

/*
 * util_parse_enums -- parse enums and store to bitmap
 */
static int
util_parse_enums(const char *str, int first, int max, uint64_t *bitmap,
		enum_to_str_fn enum_to_str)
{
	char *dup = strdup(str);
	if (!dup)
		err(1, "Cannot allocate memory for enum str");

	char *ptr = dup;
	int ret = 0;
	char *comma;
	do {
		comma = strchr(ptr, ',');
		if (comma) {
			*comma = '\0';
			comma++;
		}

		if ((ret = util_parse_enum(ptr, first, max,
				bitmap, enum_to_str))) {
			goto out;
		}

		ptr = comma;
	} while (ptr);
out:
	free(dup);
	return ret;
}

/*
 * util_parse_chunk_types -- parse chunk types strings
 */
int
util_parse_chunk_types(const char *str, uint64_t *types)
{
	assert(MAX_CHUNK_TYPE < 8 * sizeof (*types));
	return util_parse_enums(str, 0, MAX_CHUNK_TYPE, types,
			(enum_to_str_fn)out_get_chunk_type_str);
}

/*
 * util_parse_lane_section -- parse lane section strings
 */
int
util_parse_lane_sections(const char *str, uint64_t *types)
{
	assert(MAX_LANE_SECTION < 8 * sizeof (*types));
	return util_parse_enums(str, 0, MAX_LANE_SECTION, types,
			(enum_to_str_fn)out_get_lane_section_str);
}

/*
 * util_options_alloc -- allocate and initialize options structure
 */
struct options *
util_options_alloc(const struct option *options,
		size_t nopts, const struct option_requirement *req)
{
	struct options *opts = calloc(1, sizeof (*opts));
	if (!opts)
		err(1, "Cannot allocate memory for options structure");

	opts->options = options;
	opts->noptions = nopts;
	opts->req = req;
	size_t bitmap_size = howmany(nopts, 8);
	opts->bitmap = calloc(bitmap_size, 1);
	if (!opts->bitmap)
		err(1, "Cannot allocate memory for options bitmap");

	return opts;
}

/*
 * util_options_free -- free options structure
 */
void
util_options_free(struct options *opts)
{
	free(opts->bitmap);
	free(opts);
}

/*
 * util_opt_get_index -- return index of specified option in global
 * array of options
 */
static int
util_opt_get_index(const struct options *opts, int opt)
{
	const struct option *lopt = &opts->options[0];
	int ret = 0;
	while (lopt->name) {
		if ((lopt->val & ~OPT_MASK) == opt)
			return ret;
		lopt++;
		ret++;
	}
	return -1;
}

/*
 * util_opt_get_req -- get required option for specified option
 */
static struct option_requirement *
util_opt_get_req(const struct options *opts, int opt, pmem_pool_type_t type)
{
	size_t n = 0;
	struct option_requirement *ret = NULL;
	const struct option_requirement *req = &opts->req[0];
	while (req->opt) {
		if (req->opt == opt && (req->type & type)) {
			n++;
			ret = realloc(ret, n * sizeof (*ret));
			if (!ret)
				err(1, "Cannot allocate memory for"
					" option requirements");
			ret[n - 1] = *req;
		}
		req++;
	}

	if (ret) {
		ret = realloc(ret, (n + 1) * sizeof (*ret));
		if (!ret)
			err(1, "Cannot allocate memory for"
				" option requirements");
		memset(&ret[n], 0, sizeof (*ret));
	}

	return ret;
}

/*
 * util_opt_check_requirements -- check if requirements has been fulfilled
 */
static int
util_opt_check_requirements(const struct options *opts,
		const struct option_requirement *req)
{
	int count = 0;
	int set = 0;
	uint64_t tmp;
	while ((tmp = req->req) != 0) {
		while (tmp) {
			int req_idx =
				util_opt_get_index(opts, tmp & OPT_REQ_MASK);

			if (req_idx >= 0 && util_isset(opts->bitmap, req_idx)) {
				set++;
				break;
			}

			tmp >>= OPT_REQ_SHIFT;
		}
		req++;
		count++;
	}

	return count != set;
}

/*
 * util_opt_print_requirements -- print requirements for specified option
 */
static void
util_opt_print_requirements(const struct options *opts,
		const struct option_requirement *req)
{
	char buff[REQ_BUFF_SIZE];
	unsigned n = 0;
	uint64_t tmp;
	const struct option *opt =
		&opts->options[util_opt_get_index(opts, req->opt)];
	int sn;

	sn = snprintf(&buff[n], REQ_BUFF_SIZE - n,
			"option [-%c|--%s] requires: ", opt->val, opt->name);
	assert(sn >= 0);
	if (sn >= 0)
		n += (unsigned)sn;

	size_t rc = 0;
	while ((tmp = req->req) != 0) {
		if (rc != 0) {
			sn = snprintf(&buff[n], REQ_BUFF_SIZE - n, " and ");
			assert(sn >= 0);
			if (sn >= 0)
				n += (unsigned)sn;
		}

		size_t c = 0;
		while (tmp) {
			if (c == 0)
				sn = snprintf(&buff[n], REQ_BUFF_SIZE - n, "[");
			else
				sn = snprintf(&buff[n], REQ_BUFF_SIZE - n, "|");
			assert(sn >= 0);
			if (sn >= 0)
				n += (unsigned)sn;

			int req_opt_ind =
				util_opt_get_index(opts, tmp & OPT_REQ_MASK);
			const struct option *req_option =
				&opts->options[req_opt_ind];

			sn = snprintf(&buff[n], REQ_BUFF_SIZE - n,
				"-%c|--%s", req_option->val, req_option->name);
			assert(sn >= 0);
			if (sn >= 0)
				n += (unsigned)sn;

			tmp >>= OPT_REQ_SHIFT;
			c++;
		}
		sn = snprintf(&buff[n], REQ_BUFF_SIZE - n, "]");
		assert(sn >= 0);
		if (sn >= 0)
			n += (unsigned)sn;

		req++;
		rc++;
	}

	outv_err("%s\n", buff);
}

/*
 * util_opt_verify_requirements -- verify specified requirements for options
 */
static int
util_opt_verify_requirements(const struct options *opts, size_t index,
		pmem_pool_type_t type)
{
	const struct option *opt = &opts->options[index];
	int val = opt->val & ~OPT_MASK;
	struct option_requirement *req;

	if ((req = util_opt_get_req(opts, val, type)) == NULL)
		return 0;

	int ret = 0;

	if (util_opt_check_requirements(opts, req)) {
		ret = -1;
		util_opt_print_requirements(opts, req);
	}

	free(req);
	return ret;
}

/*
 * util_opt_verify_type -- check if used option matches pool type
 */
static int
util_opt_verify_type(const struct options *opts, pmem_pool_type_t type,
		size_t index)
{
	const struct option *opt = &opts->options[index];
	int val = opt->val & ~OPT_MASK;
	int opt_type = opt->val;
	opt_type >>= OPT_SHIFT;
	if (!(opt_type & (1<<type))) {
		outv_err("'--%s|-%c' -- invalid option specified"
			" for pool type '%s'\n",
			opt->name, val,
			out_get_pool_type_str(type));
		return -1;
	}

	return 0;
}

/*
 * util_options_getopt -- wrapper for getopt_long which sets bitmap
 */
int
util_options_getopt(int argc, char *argv[], const char *optstr,
		const struct options *opts)
{
	int opt = getopt_long(argc, argv, optstr, opts->options, NULL);
	if (opt == -1 || opt == '?')
		return opt;

	opt &= ~OPT_MASK;
	int option_index = util_opt_get_index(opts, opt);
	assert(option_index >= 0);

	util_setbit((uint8_t *)opts->bitmap, (unsigned)option_index);

	return opt;
}

/*
 * util_options_verify -- verify options
 */
int
util_options_verify(const struct options *opts, pmem_pool_type_t type)
{
	for (size_t i = 0; i < opts->noptions; i++) {
		if (util_isset(opts->bitmap, i)) {
			if (util_opt_verify_type(opts, type, i))
				return -1;

			if (opts->req)
				if (util_opt_verify_requirements(opts, i, type))
					return -1;
		}
	}

	return 0;
}

/*
 * util_heap_max_zone -- get number of zones
 */
unsigned
util_heap_max_zone(size_t size)
{
	unsigned max_zone = 0;
	size -= sizeof (struct heap_header);

	while (size >= ZONE_MIN_SIZE) {
		max_zone++;
		size -= size <= ZONE_MAX_SIZE ? size : ZONE_MAX_SIZE;
	}

	return max_zone;
}

/*
 * util_heap_get_bitmap_params -- return bitmap parameters of given block size
 *
 * The function returns the following values:
 * - number of allocations
 * - number of used values in bitmap
 * - initial value of last used entry
 */
int
util_heap_get_bitmap_params(uint64_t block_size, uint64_t *nallocsp,
		uint64_t *nvalsp, uint64_t *last_valp)
{
	assert(RUNSIZE / block_size <= UINT32_MAX);
	uint32_t nallocs = (uint32_t)(RUNSIZE / block_size);

	assert(nallocs <= RUN_BITMAP_SIZE);
	unsigned unused_bits = RUN_BITMAP_SIZE - nallocs;

	unsigned unused_values = unused_bits / BITS_PER_VALUE;

	assert(MAX_BITMAP_VALUES >= unused_values);
	uint64_t nvals = MAX_BITMAP_VALUES - unused_values;

	assert(unused_bits >= unused_values * BITS_PER_VALUE);
	unused_bits -= unused_values * BITS_PER_VALUE;

	uint64_t last_val = unused_bits ? (((1ULL << unused_bits) - 1ULL) <<
				(BITS_PER_VALUE - unused_bits)) : 0;

	if (nvals >= MAX_BITMAP_VALUES || nvals == 0)
		return -1;

	if (nallocsp)
		*nallocsp = nallocs;
	if (nvalsp)
		*nvalsp = nvals;
	if (last_valp)
		*last_valp = last_val;

	return 0;
}

/*
 * util_plist_nelements -- count number of elements on a list
 */
size_t
util_plist_nelements(struct pmemobjpool *pop, struct list_head *headp)
{
	size_t i = 0;
	struct list_entry *entryp;
	PLIST_FOREACH(entryp, pop, headp)
		i++;
	return i;
}

/*
 * util_plist_get_entry -- return nth element from list
 */
struct list_entry *
util_plist_get_entry(struct pmemobjpool *pop,
	struct list_head *headp, size_t n)
{
	struct list_entry *entryp;
	PLIST_FOREACH(entryp, pop, headp) {
		if (n == 0)
			return entryp;
		n--;
	}

	return NULL;
}

/*
 * pool_set_file_open -- opens pool set file or regular file
 */
struct pool_set_file *
pool_set_file_open(const char *fname,
		int rdonly, int check)
{
	struct pool_set_file *file = calloc(1, sizeof (*file));
	if (!file)
		return NULL;

	file->replica = 0;
	file->fname = strdup(fname);
	if (!file->fname)
		goto err;

	struct stat buf;

	/*
	 * The check flag indicates whether the headers from each pool
	 * set file part should be checked for valid values.
	 */
	if (check) {
		if (util_poolset_map(file->fname,
				&file->poolset, rdonly))
			goto err_free_fname;
	} else {
		if (util_pool_open_nocheck(&file->poolset, file->fname,
				rdonly, DEFAULT_HDR_SIZE))
			goto err_free_fname;
	}

	file->size = file->poolset->poolsize;

	/* get modification time from the first part of first replica */
	const char *path = file->poolset->replica[0]->part[0].path;
	if (stat(path, &buf)) {
		warn("%s", path);
		goto err_close_poolset;
	}

	file->mtime = buf.st_mtime;
	file->mode = buf.st_mode;
	file->addr = file->poolset->replica[0]->part[0].addr;

	return file;

err_close_poolset:
	util_poolset_close(file->poolset, 0);
err_free_fname:
	free(file->fname);
err:
	free(file);
	return NULL;
}

/*
 * pool_set_file_close -- closes pool set file or regular file
 */
void
pool_set_file_close(struct pool_set_file *file)
{
	if (file->poolset)
		util_poolset_close(file->poolset, 0);
	else if (file->addr) {
		munmap(file->addr, file->size);
		close(file->fd);
	}
	free(file->fname);
	free(file);
}

/*
 * pool_set_file_read -- read from pool set file or regular file
 */
int
pool_set_file_read(struct pool_set_file *file, void *buff,
		size_t nbytes, uint64_t off)
{
	if (off + nbytes > file->size)
		return -1;

	memcpy(buff, (char *)file->addr + off, nbytes);

	return 0;
}

/*
 * pool_set_file_write -- write to pool set file or regular file
 */
int
pool_set_file_write(struct pool_set_file *file, void *buff,
		size_t nbytes, uint64_t off)
{
	if (off + nbytes > file->size)
		return -1;

	memcpy((char *)file->addr + off, buff, nbytes);

	return 0;
}

/*
 * pool_set_file_set_replica -- change replica for pool set file
 */
int
pool_set_file_set_replica(struct pool_set_file *file, size_t replica)
{
	if (!replica)
		return 0;

	if (!file->poolset)
		return -1;

	if (replica >= file->poolset->nreplicas)
		return -1;

	file->replica = replica;
	file->addr = file->poolset->replica[replica]->part[0].addr;

	return 0;
}

/*
 * pool_set_file_map -- return mapped address at given offset
 */
void *
pool_set_file_map(struct pool_set_file *file, uint64_t offset)
{
	if (file->addr == MAP_FAILED)
		return NULL;
	return (char *)file->addr + offset;
}

/*
 * pool_set_file_map_headers -- map headers of each pool set part file
 */
int
pool_set_file_map_headers(struct pool_set_file *file,
		int rdonly, size_t hdrsize)
{
	if (!file->poolset)
		return -1;

	int flags = rdonly ? MAP_PRIVATE : MAP_SHARED;
	for (unsigned r = 0; r < file->poolset->nreplicas; r++) {
		struct pool_replica *rep = file->poolset->replica[r];
		for (unsigned p = 0; p < rep->nparts; p++) {
			struct pool_set_part *part = &rep->part[p];

			part->hdr = mmap(NULL, hdrsize, PROT_READ | PROT_WRITE,
					flags, part->fd, 0);
			if (part->hdr == MAP_FAILED) {
				part->hdr = NULL;
				goto err;
			}

			part->hdrsize = hdrsize;
		}
	}

	return 0;
err:
	pool_set_file_unmap_headers(file);
	return -1;
}

/*
 * pool_set_file_unmap_headers -- unmap headers of each pool set part file
 */
void
pool_set_file_unmap_headers(struct pool_set_file *file)
{
	if (!file->poolset)
		return;
	for (unsigned r = 0; r < file->poolset->nreplicas; r++) {
		struct pool_replica *rep = file->poolset->replica[r];
		for (unsigned p = 0; p < rep->nparts; p++) {
			struct pool_set_part *part = &rep->part[p];
			if (part->hdr != NULL) {
				assert(part->hdrsize > 0);
				munmap(part->hdr, part->hdrsize);
				part->hdr = NULL;
				part->hdrsize = 0;
			}
		}
	}
}
