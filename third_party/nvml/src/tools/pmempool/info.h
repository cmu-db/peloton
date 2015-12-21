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
 * info.h -- pmempool info command header file
 */


/*
 * Verbose levels used in application:
 *
 * VERBOSE_DEFAULT:
 * Default value for application's verbosity level.
 * This is also set for data structures which should be
 * printed without any command line argument.
 *
 * VERBOSE_MAX:
 * Maximum value for application's verbosity level.
 * This value is used when -v command line argument passed.
 *
 * VERBOSE_SILENT:
 * This value is higher than VERBOSE_MAX and it is used only
 * for verbosity levels of data structures which should _not_ be
 * printed without specified command line arguments.
 */
#define	VERBOSE_SILENT	0
#define	VERBOSE_DEFAULT	1
#define	VERBOSE_MAX	2

/*
 * pmempool_info_args -- structure for storing command line arguments
 */
struct pmempool_info_args {
	char *file;		/* input file */
	unsigned int col_width;	/* column width for printing fields */
	bool human;		/* sizes in human-readable formats */
	bool force;		/* force parsing pool */
	pmem_pool_type_t type;	/* forced pool type */
	bool use_range;		/* use range for blocks */
	struct ranges ranges;	/* range of block/chunks to dump */
	int vlevel;		/* verbosity level */
	int vdata;		/* verbosity level for data dump */
	int vhdrdump;		/* verbosity level for headers hexdump */
	int vstats;		/* verbosity level for statistics */
	struct {
		size_t walk;		/* data chunk size */
	} log;
	struct {
		int vmap;	/* verbosity level for BTT Map */
		int vflog;	/* verbosity level for BTT FLOG */
		int vbackup;	/* verbosity level for BTT Info backup */
		bool skip_zeros; /* skip blocks marked with zero flag */
		bool skip_error; /* skip blocks marked with error flag */
		bool skip_no_flag; /* skip blocks not marked with any flag */
	} blk;
	struct {
		int vlanes;		/* verbosity level for lanes */
		int vroot;
		int vobjects;
		int valloc;
		int voobhdr;
		int vheap;
		int vzonehdr;
		int vchunkhdr;
		int vbitmap;
		uint64_t lane_sections;
		bool lanes_recovery;
		bool ignore_empty_obj;
		uint64_t chunk_types;
		size_t replica;
		struct ranges lane_ranges;
		struct ranges object_ranges;
		struct ranges zone_ranges;
		struct ranges chunk_ranges;
	} obj;
};

/*
 * pmem_blk_stats -- structure with statistics for pmemblk
 */
struct pmem_blk_stats {
	uint32_t total;		/* number of processed blocks */
	uint32_t zeros;		/* number of blocks marked by zero flag */
	uint32_t errors;	/* number of blocks marked by error flag */
	uint32_t noflag;	/* number of blocks not marked with any flag */
};

struct pmem_obj_class_stats {
	uint64_t n_units;
	uint64_t n_used;
};

struct pmem_obj_zone_stats {
	uint64_t n_chunks;
	uint64_t n_chunks_type[MAX_CHUNK_TYPE];
	uint64_t size_chunks;
	uint64_t size_chunks_type[MAX_CHUNK_TYPE];
	struct pmem_obj_class_stats class_stats[MAX_BUCKETS];
};

struct pmem_obj_stats {
	uint64_t n_total_objects;
	uint64_t n_total_bytes;
	uint64_t n_type_objects[PMEMOBJ_NUM_OID_TYPES];
	uint64_t n_type_bytes[PMEMOBJ_NUM_OID_TYPES];
	uint64_t n_zones;
	uint64_t n_zones_used;
	struct pmem_obj_zone_stats *zone_stats;
};

/*
 * pmem_info -- context for pmeminfo application
 */
struct pmem_info {
	const char *file_name;	/* current file name */
	struct pool_set_file *pfile;
	struct pmempool_info_args args;	/* arguments parsed from command line */
	struct options *opts;
	struct pool_set *poolset;
	pmem_pool_type_t type;
	struct pmem_pool_params params;
	struct {
		struct pmem_blk_stats stats;
	} blk;
	struct {
		void *addr;		/* mapped file */
		size_t size;
		struct pmem_obj_stats stats;
		uint64_t uuid_lo;
	} obj;
};

int pmempool_info_func(char *appname, int argc, char *argv[]);
void pmempool_info_help(char *appname);

int pmempool_info_read(struct pmem_info *pip, void *buff,
		size_t nbytes, uint64_t off);
int pmempool_info_blk(struct pmem_info *pip);
int pmempool_info_log(struct pmem_info *pip);
int pmempool_info_obj(struct pmem_info *pip);
