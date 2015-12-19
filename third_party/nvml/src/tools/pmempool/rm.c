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
 * rm.c -- pmempool rm command main source file
 */

#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>
#include <err.h>
#include <stdio.h>
#include <fcntl.h>

#include "common.h"
#include "output.h"
#include "rm.h"

enum ask_type {
	ASK_SOMETIMES,	/* ask before removing write-protected files */
	ASK_ALWAYS,	/* always ask */
	ASK_NEVER,	/* never ask */
};

/* verbosity level */
static int vlevel;
/* force remove and ignore errors */
static int force;
/* remove only pool files */
static int only_pools;
/* mode of interaction */
static enum ask_type ask_mode;

/* help message */
static const char *help_str =
"Remove pool file or all files from poolset\n"
"\n"
"Available options:\n"
"  -h, --help         Print this help message.\n"
"  -v, --verbose      Be verbose.\n"
"  -s, --only-pools   Remove only pool files.\n"
"  -f, --force        Ignore nonexisting files.\n"
"  -i, --interactive  Prompt before every single removal.\n"
"\n"
"For complete documentation see %s-rm(1) manual page.\n";

/* short options string */
static const char *optstr = "hvsfi";
/* long options */
static const struct option long_options[] = {
	{"help",	no_argument,		0, 'h'},
	{"verbose",	no_argument,		0, 'v'},
	{"only-pools",	no_argument,		0, 's'},
	{"force",	no_argument,		0, 'f'},
	{"interactive",	no_argument,		0, 'i'},
	{NULL,		0,			0,  0 },
};

/*
 * print_usage -- print usage message
 */
static void
print_usage(const char *appname)
{
	printf("Usage: %s rm [<args>] <files>\n", appname);
}

/*
 * pmempool_rm_help -- print help message
 */
void
pmempool_rm_help(char *appname)
{
	print_usage(appname);
	printf(help_str, appname);
}

/*
 * rm_file -- remove single file
 */
static void
rm_file(const char *file)
{
	int write_protected = access(file, W_OK) != 0;
	char cask = 'y';
	switch (ask_mode) {
	case ASK_ALWAYS:
		cask = '?';
		break;
	case ASK_NEVER:
		cask = 'y';
		break;
	case ASK_SOMETIMES:
		cask = write_protected ? '?' : 'y';
		break;
	}

	const char *pre_msg = write_protected ? "write-protected " : "";
	if (ask_Yn(cask, "remove %sfile '%s' ?", pre_msg, file) == 'y') {
		if (unlink(file))
			err(1, "cannot remove file '%s'", file);
		outv(1, "removed '%s'\n", file);
	}
}

/*
 * rm_poolset -- remove files parsed from poolset file
 */
static void
rm_poolset(const char *file)
{
	int fd = open(file, O_RDONLY);
	if (fd < 0) {
		perror(file);
		exit(EXIT_FAILURE);
	}

	struct pool_set *set;
	int is_poolset = util_poolset_parse(file, fd, &set);
	close(fd);
	if (is_poolset) {
		if (!force) {
			outv_err("cannot parse poolset file '%s'\n", file);
			exit(1);
		}
		return;
	}

	for (unsigned r = 0; r < set->nreplicas; r++) {
		for (unsigned p = 0; p < set->replica[r]->nparts; p++) {
			const char *part_file = set->replica[r]->part[p].path;
			outv(2, "part file   : %s\n", part_file);

			int exists = access(part_file, F_OK) == 0;
			if (!exists) {
				/*
				 * Ignore not accessible file if force
				 * flag is set
				 */
				if (force)
					continue;

				err(1, "cannot remove file '%s'", part_file);
			}

			rm_file(part_file);
		}
	}

	util_poolset_free(set);
}

/*
 * pmempool_rm_func -- main function for rm command
 */
int
pmempool_rm_func(char *appname, int argc, char *argv[])
{
	int opt;
	while ((opt = getopt_long(argc, argv, optstr,
			long_options, NULL)) != -1) {
		switch (opt) {
		case 'h':
			pmempool_rm_help(appname);
			return 0;
		case 'v':
			vlevel++;
			break;
		case 's':
			only_pools = 1;
			break;
		case 'f':
			force = 1;
			ask_mode = ASK_NEVER;
			break;
		case 'i':
			ask_mode = ASK_ALWAYS;
			break;
		default:
			print_usage(appname);
			return -1;
		}
	}

	out_set_vlevel(vlevel);

	for (int i = optind; i < argc; i++) {
		char *file = argv[i];
		/* check if file exists and we can read it */
		int exists = access(file, F_OK | R_OK) == 0;
		if (!exists) {
			/* ignore not accessible file if force flag is set */
			if (force)
				continue;
			err(1, "cannot remove '%s'", file);
		}

		int is_poolset = pmem_pool_check_pool_set(file) == 0;

		if (is_poolset)
			outv(2, "poolset file: %s\n", file);
		else
			outv(2, "pool file   : %s\n", file);

		if (is_poolset) {
			rm_poolset(file);
			if (!only_pools)
				rm_file(file);
		} else {
			rm_file(file);
		}
	}

	return 0;
}
