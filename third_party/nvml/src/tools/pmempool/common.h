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
 * common.h -- declarations of common functions
 */

#include <stdint.h>
#include <sys/queue.h>
#include <stdarg.h>

#include "util.h"
#include "log.h"
#include "blk.h"
#include "libpmemobj.h"
#include "lane.h"
#include "redo.h"
#include "list.h"
#include "obj.h"
#include "heap.h"
#include "btt_layout.h"
#include "heap_layout.h"

#define	OPT_SHIFT 12
#define	OPT_MASK (~((1 << OPT_SHIFT) - 1))
#define	OPT_LOG (1 << (PMEM_POOL_TYPE_LOG + OPT_SHIFT))
#define	OPT_BLK (1 << (PMEM_POOL_TYPE_BLK + OPT_SHIFT))
#define	OPT_OBJ (1 << (PMEM_POOL_TYPE_OBJ + OPT_SHIFT))
#define	OPT_ALL (OPT_LOG | OPT_BLK | OPT_OBJ)

#define	OPT_REQ_SHIFT	8
#define	OPT_REQ_MASK	((1 << OPT_REQ_SHIFT) - 1)
#define	_OPT_REQ(c, n) ((c) << (OPT_REQ_SHIFT * (n)))
#define	OPT_REQ0(c) _OPT_REQ(c, 0)
#define	OPT_REQ1(c) _OPT_REQ(c, 1)
#define	OPT_REQ2(c) _OPT_REQ(c, 2)
#define	OPT_REQ3(c) _OPT_REQ(c, 3)
#define	OPT_REQ4(c) _OPT_REQ(c, 4)
#define	OPT_REQ5(c) _OPT_REQ(c, 5)
#define	OPT_REQ6(c) _OPT_REQ(c, 6)
#define	OPT_REQ7(c) _OPT_REQ(c, 7)

#define	min(a, b) ((a) < (b) ? (a) : (b))
#define	ENTIRE_UINT64 (\
{\
struct range _entire_uint64 = {\
	.first = 0,\
	.last = UINT64_MAX\
}; _entire_uint64; })

#define	FOREACH_RANGE(range, ranges)\
	LIST_FOREACH(range, &(ranges)->head, next)

#define	PLIST_OFF_TO_PTR(pop, off)\
((off) == 0 ? NULL : (void *)((uintptr_t)(pop) + (off) - OBJ_OOB_SIZE))

#define	PLIST_FOREACH(entry, pop, head)\
for ((entry) = PLIST_OFF_TO_PTR(pop, (head)->pe_first.off);\
	(entry) != NULL;\
	(entry) = ((entry)->pe_next.off == (head)->pe_first.off ?\
	NULL : PLIST_OFF_TO_PTR(pop, (entry)->pe_next.off)))

#define	PLIST_EMPTY(head) ((head)->pe_first.off == 0)

#define	ENTRY_TO_OOB_HDR(entry) ((struct oob_header *)(entry))

#define	ENTRY_TO_TX_RANGE(entry)\
((void *)((uintptr_t)(entry) + sizeof (struct oob_header)))

#define	ENTRY_TO_ALLOC_HDR(entry)\
((void *)((uintptr_t)(entry) - sizeof (struct allocation_header)))

#define	ENTRY_TO_DATA(entry)\
((void *)((uintptr_t)(entry) + sizeof (struct oob_header)))

#define	DEFAULT_HDR_SIZE 8192

/*
 * pmem_pool_type_t -- pool types
 */
typedef enum {
	PMEM_POOL_TYPE_LOG	= 0x01,
	PMEM_POOL_TYPE_BLK	= 0x02,
	PMEM_POOL_TYPE_OBJ	= 0x04,
	PMEM_POOL_TYPE_ALL	= 0x0f,
	PMEM_POOL_TYPE_UNKNOWN	= 0x80,
} pmem_pool_type_t;

struct option_requirement {
	char opt;
	pmem_pool_type_t type;
	uint64_t req;
};

struct options {
	const struct option *options;
	size_t noptions;
	char *bitmap;
	const struct option_requirement *req;
};

struct pmem_pool_params {
	pmem_pool_type_t type;
	char signature[POOL_HDR_SIG_LEN];
	uint64_t size;
	mode_t mode;
	int is_poolset;
	int is_part;
	union {
		struct {
			uint64_t bsize;
		} blk;
		struct {
			char layout[PMEMOBJ_MAX_LAYOUT];
		} obj;
	};
};

struct pool_set_file {
	int fd;
	char *fname;
	void *addr;
	size_t size;
	struct pool_set *poolset;
	size_t replica;
	time_t mtime;
	mode_t mode;
};

struct pool_set_file *pool_set_file_open(const char *fname,
		int rdonly, int check);
void pool_set_file_close(struct pool_set_file *file);
int pool_set_file_read(struct pool_set_file *file, void *buff,
		size_t nbytes, uint64_t off);
int pool_set_file_write(struct pool_set_file *file, void *buff,
		size_t nbytes, uint64_t off);
int pool_set_file_set_replica(struct pool_set_file *file, size_t replica);
void *pool_set_file_map(struct pool_set_file *file, uint64_t offset);
int pool_set_file_map_headers(struct pool_set_file *file,
		int rdonly, size_t hdrsize);
void pool_set_file_unmap_headers(struct pool_set_file *file);

struct range {
	LIST_ENTRY(range) next;
	uint64_t first;
	uint64_t last;
};

struct ranges {
	LIST_HEAD(rangeshead, range) head;
};

pmem_pool_type_t pmem_pool_type_parse_hdr(const struct pool_hdr *hdrp);
pmem_pool_type_t pmem_pool_type_parse_str(const char *str);
int pmem_pool_check_pool_set(const char *fname);
uint64_t pmem_pool_get_min_size(pmem_pool_type_t type);
size_t pmem_pool_get_hdr_size(pmem_pool_type_t type);
int pmem_pool_parse_params(const char *fname, struct pmem_pool_params *paramsp,
		int check);
void pmem_default_pool_hdr(pmem_pool_type_t type, struct pool_hdr *hdrp);
int pmem_pool_is_pool_set_part(const struct pool_hdr *hdrp);
int util_poolset_map(const char *fname, struct pool_set **poolset, int rdonly);
struct options *util_options_alloc(const struct option *options,
		size_t nopts, const struct option_requirement *req);
void util_options_free(struct options *opts);
int util_options_verify(const struct options *opts, pmem_pool_type_t type);
int util_options_getopt(int argc, char *argv[], const char *optstr,
		const struct options *opts);
int util_validate_checksum(void *addr, size_t len, uint64_t *csum);
int util_pool_hdr_valid(struct pool_hdr *hdrp);
int util_parse_size(const char *str, uint64_t *sizep);
int util_parse_mode(const char *str, mode_t *mode);
int util_parse_ranges(const char *str, struct ranges *rangesp,
		struct range entire);
int util_ranges_add(struct ranges *rangesp, struct range range);
void util_ranges_clear(struct ranges *rangesp);
int util_ranges_contain(const struct ranges *rangesp, uint64_t n);
int util_ranges_empty(const struct ranges *rangesp);
void util_convert2h_pool_hdr(struct pool_hdr *hdrp);
void util_convert2h_btt_info(struct btt_info *bttp);
void util_convert2le_pool_hdr(struct pool_hdr *hdrp);
void util_convert2le_btt_info(struct btt_info *bttp);
void util_convert2h_btt_flog(struct btt_flog *flogp);
void util_convert2le_btt_flog(struct btt_flog *flogp);
void util_convert2h_pmemlog(struct pmemlog *plp);
void util_convert2le_pmemlog(struct pmemlog *plp);
int util_check_memory(const uint8_t *buff, size_t len, uint8_t val);
int util_check_bsize(uint32_t bsize, uint64_t fsize);
uint32_t util_get_max_bsize(uint64_t fsize);
int util_parse_chunk_types(const char *str, uint64_t *types);
int util_parse_lane_sections(const char *str, uint64_t *types);
char ask(char op, char *answers, char def_ans, const char *fmt, va_list ap);
char ask_yn(char op, char def_ans, const char *fmt, va_list ap);
char ask_Yn(char op, const char *fmt, ...);
char ask_yN(char op, const char *fmt, ...);
unsigned util_heap_max_zone(size_t size);
int util_heap_get_bitmap_params(uint64_t block_size, uint64_t *nallocsp,
		uint64_t *nvalsp, uint64_t *last_valp);
size_t util_plist_nelements(struct pmemobjpool *pop, struct list_head *headp);
struct list_entry *util_plist_get_entry(struct pmemobjpool *pop,
	struct list_head *headp, size_t n);

static inline uint32_t
util_count_ones(uint64_t val)
{
	return (uint32_t)__builtin_popcountll(val);
}
