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
 * util_poolset.c -- unit test for util_pool_create() / util_pool_open()
 *
 * usage: util_poolset cmd minlen hdrsize [mockopts] setfile ...
 */

#include "unittest.h"
#include "util.h"
#include <errno.h>

#define	LOG_PREFIX "ut"
#define	LOG_LEVEL_VAR "TEST_LOG_LEVEL"
#define	LOG_FILE_VAR "TEST_LOG_FILE"
#define	MAJOR_VERSION 1
#define	MINOR_VERSION 0
#define	SIG "PMEMXXX"

const char *Open_path = "";
off_t Fallocate_len = -1;
size_t Is_pmem_len = 0;

/*
 * Declaration of out_init and out_fini functions because it is not
 * possible to include both unittest.h and out.h headers due to
 * redeclaration of some macros.
 */
void out_init(const char *log_prefix, const char *log_level_var,
		const char *log_file_var, int major_version,
		int minor_version);
void out_fini(void);


/*
 * poolset_info -- (internal) dumps poolset info and checks its integrity
 *
 * Performs the following checks:
 * - part_size[i] == rounddown(file_size - pool_hdr_size, Pagesize)
 * - replica_size == sum(part_size)
 * - pool_size == min(replica_size)
 */
static void
poolset_info(const char *fname, struct pool_set *set, size_t hdrsize, int o)
{
	if (o)
		OUT("%s: opened: hdrsize %zu nreps %d poolsize %zu rdonly %d",
			fname, hdrsize, set->nreplicas, set->poolsize,
			set->rdonly);
	else
		OUT("%s: created: hdrsize %zu nreps %d poolsize %zu zeroed %d",
			fname, hdrsize, set->nreplicas, set->poolsize,
			set->zeroed);

	size_t poolsize = SIZE_MAX;

	for (int r = 0; r < set->nreplicas; r++) {
		struct pool_replica *rep = set->replica[r];
		size_t repsize = 0;

		OUT("  replica[%d]: nparts %d repsize %zu is_pmem %d",
			r, rep->nparts, rep->repsize, rep->is_pmem);

		for (int i = 0; i < rep->nparts; i++) {
			struct pool_set_part *part = &rep->part[i];
			OUT("    part[%d] path %s filesize %zu size %zu",
				i, part->path, part->filesize, part->size);
			size_t partsize = (part->filesize & ~(Ut_pagesize - 1));
			repsize += partsize;
			if (i > 0)
				ASSERTeq(part->size, partsize - hdrsize);
		}

		repsize -= (rep->nparts - 1) * hdrsize;
		ASSERTeq(rep->repsize, repsize);
		ASSERTeq(rep->part[0].size, repsize);

		if (rep->repsize < poolsize)
			poolsize = rep->repsize;
	}
	ASSERTeq(set->poolsize, poolsize);
}

/*
 * mock_options -- (internal) parse mock options and enable mocked functions
 */
static int
mock_options(const char *arg)
{
	/* reset to defaults */
	Open_path = "";
	Fallocate_len = -1;
	Is_pmem_len = 0;

	if (arg[0] != '-' || arg[1] != 'm')
		return 0;

	switch (arg[2]) {
	case 'n':
		/* do nothing */
		break;
	case 'o':
		/* open */
		Open_path = &arg[4];
		break;
	case 'f':
		/* fallocate */
		Fallocate_len = atoll(&arg[4]);
		break;
	case 'p':
		/* is_pmem */
		Is_pmem_len = atoll(&arg[4]);
		break;
	default:
		FATAL("unknown mock option: %c", arg[2]);
	}

	return 1;
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "util_poolset");

	out_init(LOG_PREFIX, LOG_LEVEL_VAR, LOG_FILE_VAR,
			MAJOR_VERSION, MINOR_VERSION);
	util_init();

	if (argc < 6)
		FATAL("usage: %s cmd minlen hdrsize [mockopts] setfile ...",
			argv[0]);

	char *fname;
	struct pool_set *set;
	int ret;

	size_t minsize = strtoul(argv[2], &fname, 0);
	size_t hdrsize = strtoul(argv[3], &fname, 0);

	for (int arg = 4; arg < argc; arg++) {
		arg += mock_options(argv[arg]);
		fname = argv[arg];

		switch (argv[1][0]) {
		case 'c':
			ret = util_pool_create(&set, fname, 0, minsize, hdrsize,
				SIG, 1, 0, 0, 0);
			if (ret == -1)
				OUT("!%s: util_pool_create", fname);
			else {
				util_poolset_chmod(set, S_IWUSR | S_IRUSR);
				poolset_info(fname, set, hdrsize, 0);
				util_poolset_close(set, 0); /* do not delete */
			}
			break;
		case 'o':
			ret = util_pool_open(&set, fname, 0 /* rdonly */,
				minsize, hdrsize, SIG, 1, 0, 0, 0);
			if (ret == -1)
				OUT("!%s: util_pool_open", fname);
			else {
				poolset_info(fname, set, hdrsize, 1);
				util_poolset_close(set, 0); /* do not delete */
			}
			break;
		}
	}

	out_fini();

	DONE(NULL);
}
