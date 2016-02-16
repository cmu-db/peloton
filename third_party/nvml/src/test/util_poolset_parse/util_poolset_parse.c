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
 * obj_pool_sets_parser.c -- unit test for parsing a set file
 *
 * usage: obj_pool_sets_parser set-file ...
 */

#include "unittest.h"
#include "util.h"

#define	LOG_PREFIX "parser"
#define	LOG_LEVEL_VAR "PARSER_LOG_LEVEL"
#define	LOG_FILE_VAR "PARSER_LOG_FILE"
#define	MAJOR_VERSION 1
#define	MINOR_VERSION 0

/*
 * Declaration of out_init and out_fini functions because it is not
 * possible to include both unittest.h and out.h headers due to
 * redeclaration of some macros.
 */
void out_init(const char *log_prefix, const char *log_level_var,
		const char *log_file_var, int major_version,
		int minor_version);
void out_fini(void);

int
main(int argc, char *argv[])
{
	START(argc, argv, "util_poolset_parse");

	out_init(LOG_PREFIX, LOG_LEVEL_VAR, LOG_FILE_VAR,
			MAJOR_VERSION, MINOR_VERSION);

	if (argc < 2)
		FATAL("usage: %s set-file-name ...", argv[0]);

	struct pool_set *set;
	int fd;

	for (int i = 1; i < argc; i++) {
		const char *path = argv[i];

		fd = OPEN(path, O_RDWR);

		int ret = util_poolset_parse(path, fd, &set);
		if (ret == 0)
			util_poolset_free(set);

		CLOSE(fd);
	}

	out_fini();

	DONE(NULL);
}
