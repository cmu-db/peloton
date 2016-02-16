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
 * traces.c -- unit test for traces
 */

#define	LOG_PREFIX "trace"
#define	LOG_LEVEL_VAR "TRACE_LOG_LEVEL"
#define	LOG_FILE_VAR "TRACE_LOG_FILE"
#define	MAJOR_VERSION 1
#define	MINOR_VERSION 0

#include <sys/types.h>
#include <stdarg.h>
#include "unittest.h"
#undef ERR
#undef FATAL
#undef ASSERT
#undef ASSERTinfo
#undef ASSERTeq
#undef ASSERTne
#include "out.h"

int
main(int argc, char *argv[])
{
	START(argc, argv, "out_err");

	/* Execute test */
	out_init(LOG_PREFIX, LOG_LEVEL_VAR, LOG_FILE_VAR,
			MAJOR_VERSION, MINOR_VERSION);

	errno = 0;
	ERR("ERR #%d", 1);
	OUT("%s", out_get_errormsg());

	errno = 0;
	ERR("!ERR #%d", 2);
	OUT("%s", out_get_errormsg());

	errno = EINVAL;
	ERR("!ERR #%d", 3);
	OUT("%s", out_get_errormsg());

	errno = EBADF;
	out_err(__FILE__, 100, __func__,
		"ERR1: %s:%d", strerror(errno), 1234);
	OUT("%s", out_get_errormsg());

	errno = EBADF;
	out_err(NULL, 0, NULL,
		"ERR2: %s:%d", strerror(errno), 1234);
	OUT("%s", out_get_errormsg());

	/* Cleanup */
	out_fini();

	DONE(NULL);
}
