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
 * arch_flags.c -- unit test for architecture flags
 */
#include <string.h>
#include <elf.h>
#include <link.h>

#include "unittest.h"
#include "util.h"

#define	ELF_FILE_NAME "/proc/self/exe"
#define	FATAL_USAGE()\
FATAL("usage: arch_flags <file>:<err>:<alignemnt_desc>:<reserved> <file>")
#define	ARCH_FLAGS_LOG_PREFIX "arch_flags"
#define	ARCH_FLAGS_LOG_LEVEL_VAR "ARCH_FLAGS_LOG_LEVEL"
#define	ARCH_FLAGS_LOG_FILE_VAR "ARCH_FLAGS_LOG_FILE"
#define	ARCH_FLAGS_LOG_MAJOR 0
#define	ARCH_FLAGS_LOG_MINOR 0

/*
 * Open_ret -- fake return value in open syscall
 */
int Open_ret;

/*
 * Open_path -- fake path in open syscall
 */
char *Open_path;

/*
 * Open_fake_ret -- do fake return value in open syscall
 */
int Open_fake_ret;

/*
 * Open_fake_path -- do fake path in open syscall
 */
int Open_fake_path;

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
 * open -- open syscall mock
 */
FUNC_MOCK(open, int, const char *pathname, int flags, mode_t mode)
	FUNC_MOCK_RUN_DEFAULT {
		if (strcmp(pathname, ELF_FILE_NAME) == 0) {
			if (Open_ret) {
				errno = Open_ret;
				return -1;
			}

			if (Open_path != NULL) {
				return __real_open(Open_path, flags, mode);
			}
		}

		return __real_open(pathname, flags, mode);
	}
FUNC_MOCK_END

/*
 * split_opts_path -- split options string and path to file
 *
 * The valid format is <char>:<opts>:<path>
 */
static int
split_path_opts(char *arg, char **path, char **opts)
{
	*path = arg;
	char *ptr = strchr(arg, ':');
	if (!ptr)
		return -1;
	*ptr = '\0';
	*opts = ptr + 1;
	return 0;
}

/*
 * read_arch_flags -- read arch flags from file
 */
static int
read_arch_flags(char *arg, struct arch_flags *arch_flags)
{
	char *path;
	char *opts;
	int ret;

	if ((ret = split_path_opts(arg, &path, &opts)))
		return ret;

	int error;
	uint64_t alignment_desc;
	uint64_t reserved;

	if ((ret = sscanf(opts, "%d:0x%jx:0x%jx", &error,
			&alignment_desc, &reserved)) != 3)
		return -1;

	Open_path = path;
	Open_ret = error;

	ret = util_get_arch_flags(arch_flags);
	OUT("get  : %d", ret);

	if (ret)
		return 1;

	if (alignment_desc)
		arch_flags->alignment_desc = alignment_desc;

	if (reserved)
		memcpy(arch_flags->reserved,
				&reserved, sizeof (arch_flags->reserved));

	return 0;
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "arch_flags");

	out_init(ARCH_FLAGS_LOG_PREFIX,
		ARCH_FLAGS_LOG_LEVEL_VAR,
		ARCH_FLAGS_LOG_FILE_VAR,
		ARCH_FLAGS_LOG_MAJOR,
		ARCH_FLAGS_LOG_MINOR);

	if (argc < 3)
		FATAL_USAGE();

	int i;
	for (i = 1; i < argc - 1; i += 2) {
		char *arg1 = argv[i];
		char *arg2 = argv[i + 1];
		int ret;
		struct arch_flags arch_flags;

		if ((ret = read_arch_flags(arg1, &arch_flags)) < 0)
			FATAL_USAGE();
		else if (ret == 0) {
			Open_path = arg2;
			Open_ret = 0;
			ret = util_check_arch_flags(&arch_flags);
			OUT("check: %d", ret);
		}
	}

	out_fini();

	DONE(NULL);
}
