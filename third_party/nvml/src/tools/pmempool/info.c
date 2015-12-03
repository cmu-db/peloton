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
 * info.c -- pmempool info command main source file
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdbool.h>
#include <err.h>
#include <errno.h>
#include <inttypes.h>
#include <assert.h>
#include <sys/param.h>
#define	__USE_UNIX98
#include <unistd.h>

#include "common.h"
#include "output.h"
#include "info.h"

#define	DEFAULT_CHUNK_TYPES\
	((1<<CHUNK_TYPE_FREE)|\
	(1<<CHUNK_TYPE_USED)|\
	(1<<CHUNK_TYPE_RUN))

#define	DEFAULT_LANE_SECTIONS\
	((1<<LANE_SECTION_ALLOCATOR)|\
	(1<<LANE_SECTION_TRANSACTION)|\
	(1<<LANE_SECTION_LIST))

#define	ENTIRE_TYPE_NUM (\
{\
struct range ret = {\
	.first = 0,\
	.last = PMEMOBJ_NUM_OID_TYPES - 1,\
}; ret; })

#define	GET_ALIGNMENT(ad, x)\
(1 + (((ad) >> (ALIGNMENT_DESC_BITS * (x))) & ((1 << ALIGNMENT_DESC_BITS) - 1)))

/*
 * Default arguments
 */
static const struct pmempool_info_args pmempool_info_args_default = {
	/*
	 * Picked experimentally based on used fields names.
	 * This should be at least the number of characters of
	 * the longest field name.
	 */
	.col_width	= 24,
	.human		= false,
	.force		= false,
	.type		= PMEM_POOL_TYPE_UNKNOWN,
	.vlevel		= VERBOSE_DEFAULT,
	.vdata		= VERBOSE_SILENT,
	.vhdrdump	= VERBOSE_SILENT,
	.vstats		= VERBOSE_SILENT,
	.log		= {
		.walk		= 0,
	},
	.blk		= {
		.vmap		= VERBOSE_SILENT,
		.vflog		= VERBOSE_SILENT,
		.vbackup	= VERBOSE_SILENT,
		.skip_zeros	= false,
		.skip_error	= false,
		.skip_no_flag	= false,
	},
	.obj		= {
		.vlanes		= VERBOSE_SILENT,
		.vroot		= VERBOSE_SILENT,
		.vobjects	= VERBOSE_SILENT,
		.valloc		= VERBOSE_SILENT,
		.voobhdr	= VERBOSE_SILENT,
		.vheap		= VERBOSE_SILENT,
		.vzonehdr	= VERBOSE_SILENT,
		.vchunkhdr	= VERBOSE_SILENT,
		.vbitmap	= VERBOSE_SILENT,
		.lane_sections	= DEFAULT_LANE_SECTIONS,
		.lanes_recovery	= false,
		.ignore_empty_obj = false,
		.chunk_types	= DEFAULT_CHUNK_TYPES,
		.replica	= 0,
	},
};

/*
 * long-options -- structure holding long options.
 */
static const struct option long_options[] = {
	{"version",	no_argument,		0, 'V' | OPT_ALL},
	{"verbose",	no_argument,		0, 'v' | OPT_ALL},
	{"help",	no_argument,		0, 'h' | OPT_ALL},
	{"human",	no_argument,		0, 'n' | OPT_ALL},
	{"force",	required_argument,	0, 'f' | OPT_ALL},
	{"data",	no_argument,		0, 'd' | OPT_ALL},
	{"headers-hex",	no_argument,		0, 'x' | OPT_ALL},
	{"stats",	no_argument,		0, 's' | OPT_ALL},
	{"range",	required_argument,	0, 'r' | OPT_ALL},
	{"walk",	required_argument,	0, 'w' | OPT_LOG},
	{"skip-zeros",	no_argument,		0, 'z' | OPT_BLK},
	{"skip-error",	no_argument,		0, 'e' | OPT_BLK},
	{"skip-no-flag", no_argument,		0, 'u' | OPT_BLK},
	{"map",		no_argument,		0, 'm' | OPT_BLK},
	{"flog",	no_argument,		0, 'g' | OPT_BLK},
	{"backup",	no_argument,		0, 'B' | OPT_BLK},
	{"lanes",	no_argument,		0, 'l' | OPT_OBJ},
	{"recovery",	no_argument,		0, 'R' | OPT_OBJ},
	{"section",	required_argument,	0, 'S' | OPT_OBJ},
	{"object-store", no_argument,		0, 'O' | OPT_OBJ},
	{"types",	required_argument,	0, 't' | OPT_OBJ},
	{"no-empty",	no_argument,		0, 'E' | OPT_OBJ},
	{"alloc-header", no_argument,		0, 'A' | OPT_OBJ},
	{"oob-header",	no_argument,		0, 'a' | OPT_OBJ},
	{"root",	no_argument,		0, 'o' | OPT_OBJ},
	{"heap",	no_argument,		0, 'H' | OPT_OBJ},
	{"zones",	no_argument,		0, 'Z' | OPT_OBJ},
	{"chunks",	no_argument,		0, 'C' | OPT_OBJ},
	{"chunk-type",	required_argument,	0, 'T' | OPT_OBJ},
	{"bitmap",	no_argument,		0, 'b' | OPT_OBJ},
	{"replica",	required_argument,	0, 'p' | OPT_OBJ},
	{NULL,		0,			0,  0 },
};

static const struct option_requirement option_requirements[] = {
	{
		.opt	= 'r',
		.type	= PMEM_POOL_TYPE_LOG,
		.req	= OPT_REQ0('d')
	},
	{
		.opt	= 'r',
		.type	= PMEM_POOL_TYPE_BLK,
		.req	= OPT_REQ0('d') | OPT_REQ1('m')
	},
	{
		.opt	= 'z',
		.type	= PMEM_POOL_TYPE_BLK,
		.req	= OPT_REQ0('d') | OPT_REQ1('m')
	},
	{
		.opt	= 'e',
		.type	= PMEM_POOL_TYPE_BLK,
		.req	= OPT_REQ0('d') | OPT_REQ1('m')
	},
	{
		.opt	= 'u',
		.type	= PMEM_POOL_TYPE_BLK,
		.req	= OPT_REQ0('d') | OPT_REQ1('m')
	},
	{
		.opt	= 'r',
		.type	= PMEM_POOL_TYPE_OBJ,
		.req	= OPT_REQ0('O') | OPT_REQ1('Z') |
			OPT_REQ2('C') | OPT_REQ3('l'),
	},
	{
		.opt	= 'R',
		.type	= PMEM_POOL_TYPE_OBJ,
		.req	= OPT_REQ0('l')
	},
	{
		.opt	= 'S',
		.type	= PMEM_POOL_TYPE_OBJ,
		.req	= OPT_REQ0('l')
	},
	{
		.opt	= 'E',
		.type	= PMEM_POOL_TYPE_OBJ,
		.req	= OPT_REQ0('O')
	},
	{
		.opt	= 'T',
		.type	= PMEM_POOL_TYPE_OBJ,
		.req	= OPT_REQ0('C')
	},
	{
		.opt	= 'b',
		.type	= PMEM_POOL_TYPE_OBJ,
		.req	= OPT_REQ0('H')
	},
	{
		.opt	= 'b',
		.type	= PMEM_POOL_TYPE_OBJ,
		.req	= OPT_REQ0('C')
	},
	{
		.opt	= 'A',
		.type	= PMEM_POOL_TYPE_OBJ,
		.req	= OPT_REQ0('O') | OPT_REQ1('l') | OPT_REQ2('o')
	},
	{
		.opt	= 'a',
		.type	= PMEM_POOL_TYPE_OBJ,
		.req	= OPT_REQ0('O') | OPT_REQ1('l') | OPT_REQ2('o')
	},
	{
		.opt	= 't',
		.type	= PMEM_POOL_TYPE_OBJ,
		.req	= OPT_REQ0('O') | OPT_REQ1('s'),
	},
	{
		.opt	= 'C',
		.type	= PMEM_POOL_TYPE_OBJ,
		.req	= OPT_REQ0('O') | OPT_REQ1('H') | OPT_REQ2('s'),
	},
	{
		.opt	= 'Z',
		.type	= PMEM_POOL_TYPE_OBJ,
		.req	= OPT_REQ0('O') | OPT_REQ1('H') | OPT_REQ2('s'),
	},
	{
		.opt	= 'd',
		.type	= PMEM_POOL_TYPE_OBJ,
		.req	= OPT_REQ0('O') | OPT_REQ1('o'),
	},
	{ 0,  0, 0}
};

/*
 * help_str -- string for help message
 */
static const char *help_str =
"Show information about pmem pool from specified file.\n"
"\n"
"Common options:\n"
"  -h, --help                      Print this help and exit.\n"
"  -V, --version                   Print version and exit.\n"
"  -v, --verbose                   Increase verbisity level.\n"
"  -f, --force blk|log|obj         Force parsing a pool of specified type.\n"
"  -n, --human                     Print sizes in human readable format.\n"
"  -x, --headers-hex               Hexdump all headers.\n"
"  -d, --data                      Dump log data and blocks.\n"
"  -s, --stats                     Print statistics.\n"
"  -r, --range <range>             Range of blocks/chunks/objects.\n"
"\n"
"Options for PMEMLOG:\n"
"  -w, --walk <size>               Chunk size.\n"
"\n"
"Options for PMEMBLK:\n"
"  -m, --map                       Print BTT Map entries.\n"
"  -g, --flog                      Print BTT FLOG entries.\n"
"  -B, --backup                    Print BTT Info header backup.\n"
"  -z, --skip-zeros                Skip blocks marked with zero flag.\n"
"  -e, --skip-error                Skip blocks marked with error flag.\n"
"  -u, --skip-no-flag              Skip blocks not marked with any flag.\n"
"\n"
"Options for PMEMOBJ:\n"
"  -l, --lanes [<range>]           Print lanes from specified range.\n"
"  -R, --recovery                  Print only lanes which need recovery.\n"
"  -S, --section tx,allocator,list Print only specified sections.\n"
"  -O, --object-store              Print object store.\n"
"  -t, --types <range>             Specify objects' type numbers range.\n"
"  -E, --no-empty                  Print only non-empty object store lists.\n"
"  -o, --root                      Print root object information\n"
"  -A, --alloc-header              Print allocation header for objects in\n"
"                                  object store.\n"
"  -a, --oob-header                Print OOB header\n"
"  -H, --heap                      Print heap header.\n"
"  -Z, --zones [<range>]           Print zones header. If range is specified\n"
"                                  and --object|-O option is specified prints\n"
"                                  objects from specified zones only.\n"
"  -C, --chunks [<range>]          Print zones header. If range is specified\n"
"                                  and --object|-O option is specified prints\n"
"                                  objects from specified zones only.\n"
"  -T, --chunk-type used,free,run,footer\n"
"                                  Print only specified type(s) of chunk.\n"
"                                  [requires --chunks|-C]\n"
"  -b, --bitmap                    Print chunk run's bitmap in graphical\n"
"                                  format. [requires --chunks|-C]\n"
"  -p, --replica <num>             Print info from specified replica\n"
"For complete documentation see %s-info(1) manual page.\n"
;

/*
 * print_usage -- print application usage short description
 */
static void
print_usage(char *appname)
{
	printf("Usage: %s info [<args>] <file>\n", appname);
}

/*
 * print_version -- print version string
 */
static void
print_version(char *appname)
{
	printf("%s %s\n", appname, SRCVERSION);
}

/*
 * pmempool_info_help -- print application usage detailed description
 */
void
pmempool_info_help(char *appname)
{
	print_usage(appname);
	print_version(appname);
	printf(help_str, appname);
}

/*
 * parse_args -- parse command line arguments
 *
 * Parse command line arguments and store them in pmempool_info_args
 * structure.
 * Terminates process if invalid arguments passed.
 */
static int
parse_args(char *appname, int argc, char *argv[],
		struct pmempool_info_args *argsp,
		struct options *opts)
{
	int opt;
	if (argc == 1) {
		print_usage(appname);

		return -1;
	}

	struct ranges *rangesp = &argsp->ranges;
	while ((opt = util_options_getopt(argc, argv,
			"vhnf:ezuF:L:c:dmxVw:gBsr:lRS:OECZHT:bot:aAp:",
			opts)) != -1) {

		switch (opt) {
		case 'v':
			argsp->vlevel = VERBOSE_MAX;
			break;
		case 'V':
			print_version(appname);
			exit(EXIT_SUCCESS);
		case 'h':
			pmempool_info_help(appname);
			exit(EXIT_SUCCESS);
		case 'n':
			argsp->human = true;
			break;
		case 'f':
			argsp->type = pmem_pool_type_parse_str(optarg);
			if (argsp->type == PMEM_POOL_TYPE_UNKNOWN) {
				outv_err("'%s' -- unknown pool type\n", optarg);
				return -1;
			}
			argsp->force = true;
			break;
		case 'e':
			argsp->blk.skip_error = true;
			break;
		case 'z':
			argsp->blk.skip_zeros = true;
			break;
		case 'u':
			argsp->blk.skip_no_flag = true;
			break;
		case 'r':
			if (util_parse_ranges(optarg, rangesp, ENTIRE_UINT64)) {
				outv_err("'%s' -- cannot parse range(s)\n",
						optarg);
				return -1;
			}

			if (rangesp == &argsp->ranges)
				argsp->use_range = 1;

			break;
		case 'd':
			argsp->vdata = VERBOSE_DEFAULT;
			break;
		case 'm':
			argsp->blk.vmap = VERBOSE_DEFAULT;
			break;
		case 'g':
			argsp->blk.vflog = VERBOSE_DEFAULT;
			break;
		case 'B':
			argsp->blk.vbackup = VERBOSE_DEFAULT;
			break;
		case 'x':
			argsp->vhdrdump = VERBOSE_DEFAULT;
			break;
		case 's':
			argsp->vstats = VERBOSE_DEFAULT;
			break;
		case 'w':
			argsp->log.walk = (size_t)atoll(optarg);
			if (argsp->log.walk == 0) {
				outv_err("'%s' -- invalid chunk size\n",
					optarg);
				return -1;
			}
			break;
		case 'l':
			argsp->obj.vlanes = VERBOSE_DEFAULT;
			rangesp = &argsp->obj.lane_ranges;
			break;
		case 'R':
			argsp->obj.lanes_recovery = true;
			break;
		case 'S':
			argsp->obj.lane_sections = 0;
			if (util_parse_lane_sections(optarg,
						&argsp->obj.lane_sections)) {
				outv_err("'%s' -- cannot parse"
					" lane section(s)\n", optarg);
				return -1;
			}
			break;
		case 'O':
			argsp->obj.vobjects = VERBOSE_DEFAULT;
			rangesp = &argsp->ranges;
			break;
		case 'a':
			argsp->obj.voobhdr = VERBOSE_DEFAULT;
			break;
		case 'A':
			argsp->obj.valloc = VERBOSE_DEFAULT;
			break;
		case 'E':
			argsp->obj.ignore_empty_obj = true;
			break;
		case 'Z':
			argsp->obj.vzonehdr = VERBOSE_DEFAULT;
			rangesp = &argsp->obj.zone_ranges;
			break;
		case 'C':
			argsp->obj.vchunkhdr = VERBOSE_DEFAULT;
			rangesp = &argsp->obj.chunk_ranges;
			break;
		case 'H':
			argsp->obj.vheap = VERBOSE_DEFAULT;
			break;
		case 'T':
			argsp->obj.chunk_types = 0;
			if (util_parse_chunk_types(optarg,
					&argsp->obj.chunk_types) ||
				(argsp->obj.chunk_types &
				(1 << CHUNK_TYPE_UNKNOWN))) {
				outv_err("'%s' -- cannot parse chunk type(s)\n",
						optarg);
				return -1;
			}
			break;
		case 'o':
			argsp->obj.vroot = VERBOSE_DEFAULT;
			break;
		case 't':
			if (util_parse_ranges(optarg,
				&argsp->obj.object_ranges, ENTIRE_TYPE_NUM)) {
				outv_err("'%s' -- cannot parse range(s)\n",
						optarg);
				return -1;
			}
			break;
		case 'b':
			argsp->obj.vbitmap = VERBOSE_DEFAULT;
			break;
		case 'p':
		{
			long long ll = atoll(optarg);
			if (ll < 0) {
				outv_err("'%s' -- replica cannot be negative\n",
						optarg);
				return -1;
			}
			argsp->obj.replica = (size_t)ll;
			break;
		}
		default:
			print_usage(appname);
			return -1;
		}
	}

	if (optind < argc) {
		argsp->file = argv[optind];
	} else {
		print_usage(appname);
		return -1;
	}

	if (!argsp->use_range)
		util_ranges_add(&argsp->ranges, ENTIRE_UINT64);

	if (util_ranges_empty(&argsp->obj.object_ranges))
		util_ranges_add(&argsp->obj.object_ranges, ENTIRE_TYPE_NUM);

	if (util_ranges_empty(&argsp->obj.lane_ranges))
		util_ranges_add(&argsp->obj.lane_ranges, ENTIRE_UINT64);

	if (util_ranges_empty(&argsp->obj.zone_ranges))
		util_ranges_add(&argsp->obj.zone_ranges, ENTIRE_UINT64);

	if (util_ranges_empty(&argsp->obj.chunk_ranges))
		util_ranges_add(&argsp->obj.chunk_ranges, ENTIRE_UINT64);

	return 0;
}

/*
 * pmempool_info_read -- read data from file
 */
int
pmempool_info_read(struct pmem_info *pip, void *buff, size_t nbytes,
		uint64_t off)
{
	return pool_set_file_read(pip->pfile, buff, nbytes, off);
}

/*
 * pmempool_info_pool_hdr -- print pool header information
 */
static int
pmempool_info_pool_hdr(struct pmem_info *pip, int v)
{
	static const char *alignment_desc_str[] = {
		"  char",
		"  short",
		"  int",
		"  long",
		"  long long",
		"  size_t",
		"  off_t",
		"  float",
		"  double",
		"  long double",
		"  void *",
	};
	static const size_t alignment_desc_n =
		sizeof (alignment_desc_str) / sizeof (alignment_desc_str[0]);

	int ret = 0;
	struct pool_hdr *hdr = malloc(sizeof (struct pool_hdr));
	if (!hdr)
		err(1, "Cannot allocate memory for pool_hdr");

	if (pmempool_info_read(pip, hdr, sizeof (*hdr), 0)) {
		outv_err("cannot read pool header\n");
		free(hdr);
		return -1;
	}

	struct arch_flags arch_flags;
	if (util_get_arch_flags(&arch_flags)) {
		outv_err("cannot read architecture flags\n");
		free(hdr);
		return -1;
	}

	outv(v, "POOL Header:\n");
	outv_hexdump(pip->args.vhdrdump, hdr, sizeof (*hdr), 0, 1);

	util_convert2h_pool_hdr(hdr);

	outv_field(v, "Signature", "%.*s%s", POOL_HDR_SIG_LEN,
			hdr->signature,
			pip->params.is_part ?
			" [part file]" : "");
	outv_field(v, "Major", "%d", hdr->major);
	outv_field(v, "Mandatory features", "0x%x", hdr->incompat_features);
	outv_field(v, "Not mandatory features", "0x%x", hdr->compat_features);
	outv_field(v, "Forced RO", "0x%x", hdr->ro_compat_features);
	outv_field(v, "Pool set UUID", "%s",
				out_get_uuid_str(hdr->poolset_uuid));
	outv_field(v, "UUID", "%s", out_get_uuid_str(hdr->uuid));
	outv_field(v, "Previous part UUID", "%s",
				out_get_uuid_str(hdr->prev_part_uuid));
	outv_field(v, "Next part UUID", "%s",
				out_get_uuid_str(hdr->next_part_uuid));
	outv_field(v, "Previous replica UUID", "%s",
				out_get_uuid_str(hdr->prev_repl_uuid));
	outv_field(v, "Next replica UUID", "%s",
				out_get_uuid_str(hdr->next_repl_uuid));
	outv_field(v, "Creation Time", "%s",
			out_get_time_str((time_t)hdr->crtime));

	uint64_t ad = hdr->arch_flags.alignment_desc;
	uint64_t cur_ad = arch_flags.alignment_desc;

	outv_field(v, "Alignment Descriptor", "%s",
			out_get_alignment_desc_str(ad, cur_ad));

	for (size_t i = 0; i < alignment_desc_n; i++) {
		uint64_t a = GET_ALIGNMENT(ad, i);
		if (ad == cur_ad) {
			outv_field(v + 1, alignment_desc_str[i],
					"%2d", a);
		} else {
			uint64_t av = GET_ALIGNMENT(cur_ad, i);
			if (a == av) {
				outv_field(v + 1, alignment_desc_str[i],
					"%2d [OK]", a);
			} else {
				outv_field(v + 1, alignment_desc_str[i],
					"%2d [wrong! should be %2d]", a, av);
			}
		}
	}

	outv_field(v, "Class", "%s",
			out_get_ei_class_str(hdr->arch_flags.ei_class));
	outv_field(v, "Data", "%s",
			out_get_ei_data_str(hdr->arch_flags.ei_data));
	outv_field(v, "Machine", "%s",
			out_get_e_machine_str(hdr->arch_flags.e_machine));

	outv_field(v, "Checksum", "%s", out_get_checksum(hdr, sizeof (*hdr),
				&hdr->checksum));

	free(hdr);

	return ret;
}

/*
 * pmempool_info_file -- print info about single file
 */
static int
pmempool_info_file(struct pmem_info *pip, const char *file_name)
{
	int ret = 0;

	pip->file_name = file_name;

	/*
	 * If force flag is set 'types' fields _must_ hold
	 * single pool type - this is validated when processing
	 * command line arguments.
	 */
	if (pip->args.force) {
		pip->type = pip->args.type;
	} else {
		if (pmem_pool_parse_params(file_name, &pip->params, 1)) {
			if (errno)
				perror(file_name);
			else
				outv_err("%s: cannot determine type of pool\n",
					file_name);
			return -1;
		}

		pip->type = pip->params.type;
	}

	if (PMEM_POOL_TYPE_UNKNOWN == pip->type) {
		ret = -1;
		outv_err("%s: unknown pool type -- '%s'\n", file_name,
				pip->params.signature);
	} else {
		if (util_options_verify(pip->opts, pip->type))
			return -1;

		pip->pfile = pool_set_file_open(file_name, 1, 1);
		if (!pip->pfile) {
			perror(file_name);
			return -1;
		}

		if (pip->args.obj.replica &&
			pool_set_file_set_replica(pip->pfile,
				pip->args.obj.replica)) {
			outv_err("invalid replica number '%lu'",
					pip->args.obj.replica);
		}

		if (pmempool_info_pool_hdr(pip, VERBOSE_DEFAULT)) {
			ret = -1;
			goto out_close;
		}

		if (pip->params.is_part) {
			ret = 0;
			goto out_close;
		}

		switch (pip->type) {
		case PMEM_POOL_TYPE_LOG:
			ret = pmempool_info_log(pip);
			break;
		case PMEM_POOL_TYPE_BLK:
			ret = pmempool_info_blk(pip);
			break;
		case PMEM_POOL_TYPE_OBJ:
			ret = pmempool_info_obj(pip);
			break;
		case PMEM_POOL_TYPE_UNKNOWN:
		default:
			ret = -1;
			break;
		}
out_close:
		pool_set_file_close(pip->pfile);
	}

	return ret;
}

/*
 * pmempool_info_alloc -- allocate pmem info context
 */
static struct pmem_info *
pmempool_info_alloc(void)
{
	struct pmem_info *pip = malloc(sizeof (struct pmem_info));
	if (!pip)
		err(1, "Cannot allocate memory for pmempool info context");

	if (pip) {
		memset(pip, 0, sizeof (*pip));

		/* set default command line parameters */
		memcpy(&pip->args, &pmempool_info_args_default,
				sizeof (pip->args));
		pip->opts = util_options_alloc(long_options,
				sizeof (long_options) /
				sizeof (long_options[0]),
				option_requirements);

		LIST_INIT(&pip->args.ranges.head);
		LIST_INIT(&pip->args.obj.object_ranges.head);
		LIST_INIT(&pip->args.obj.lane_ranges.head);
		LIST_INIT(&pip->args.obj.zone_ranges.head);
		LIST_INIT(&pip->args.obj.chunk_ranges.head);
	}

	return pip;
}

/*
 * pmempool_info_free -- free pmem info context
 */
static void
pmempool_info_free(struct pmem_info *pip)
{
	if (pip->obj.stats.zone_stats)
		free(pip->obj.stats.zone_stats);
	util_options_free(pip->opts);
	util_ranges_clear(&pip->args.ranges);
	util_ranges_clear(&pip->args.obj.object_ranges);
	util_ranges_clear(&pip->args.obj.zone_ranges);
	util_ranges_clear(&pip->args.obj.chunk_ranges);
	util_ranges_clear(&pip->args.obj.lane_ranges);
	free(pip);
}

int
pmempool_info_func(char *appname, int argc, char *argv[])
{
	int ret = 0;
	struct pmem_info *pip = pmempool_info_alloc();

	/* read command line arguments */
	if ((ret = parse_args(appname, argc, argv, &pip->args,
					pip->opts)) == 0) {
		/* set some output format values */
		out_set_vlevel(pip->args.vlevel);
		out_set_col_width(pip->args.col_width);

		ret = pmempool_info_file(pip, pip->args.file);
	}

	pmempool_info_free(pip);

	return ret;
}
