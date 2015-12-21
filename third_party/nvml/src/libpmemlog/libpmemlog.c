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
 * libpmemlog.c -- pmem entry points for libpmemlog
 */

#include <stdio.h>
#include <stdint.h>

#include "libpmemlog.h"

#include "util.h"
#include "out.h"
#include "log.h"

/*
 * log_init -- load-time initialization for log
 *
 * Called automatically by the run-time loader.
 */
__attribute__((constructor))
static void
libpmemlog_init(void)
{
	out_init(PMEMLOG_LOG_PREFIX, PMEMLOG_LOG_LEVEL_VAR,
			PMEMLOG_LOG_FILE_VAR, PMEMLOG_MAJOR_VERSION,
			PMEMLOG_MINOR_VERSION);
	LOG(3, NULL);
	util_init();
}

/*
 * libpmemlog_fini -- libpmemlog cleanup routine
 *
 * Called automatically when the process terminates.
 */
__attribute__((destructor))
static void
libpmemlog_fini(void)
{
	LOG(3, NULL);
	out_fini();
}

/*
 * pmemlog_check_version -- see if lib meets application version requirements
 */
const char *
pmemlog_check_version(unsigned major_required, unsigned minor_required)
{
	LOG(3, "major_required %u minor_required %u",
			major_required, minor_required);

	if (major_required != PMEMLOG_MAJOR_VERSION) {
		ERR("libpmemlog major version mismatch (need %u, found %u)",
			major_required, PMEMLOG_MAJOR_VERSION);
		return out_get_errormsg();
	}

	if (minor_required > PMEMLOG_MINOR_VERSION) {
		ERR("libpmemlog minor version mismatch (need %u, found %u)",
			minor_required, PMEMLOG_MINOR_VERSION);
		return out_get_errormsg();
	}

	return NULL;
}

/*
 * pmemlog_set_funcs -- allow overriding libpmemlog's call to malloc, etc.
 */
void
pmemlog_set_funcs(
		void *(*malloc_func)(size_t size),
		void (*free_func)(void *ptr),
		void *(*realloc_func)(void *ptr, size_t size),
		char *(*strdup_func)(const char *s))
{
	LOG(3, NULL);

	util_set_alloc_funcs(malloc_func, free_func, realloc_func, strdup_func);
}

/*
 * pmemlog_errormsg -- return last error message
 */
const char *
pmemlog_errormsg(void)
{
	return out_get_errormsg();
}
