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
 * traces_custom_function.c -- unit test for traces with custom print or
 * vsnprintf functions
 *
 * usage: traces_custom_function [v|p]
 *
 */

#define	LOG_PREFIX "trace_func"
#define	LOG_LEVEL_VAR "TRACE_LOG_LEVEL"
#define	LOG_FILE_VAR "TRACE_LOG_FILE"
#define	MAJOR_VERSION 1
#define	MINOR_VERSION 0

#include <sys/types.h>
#include <stdarg.h>
#include "out.h"
#undef ERR
#undef FATAL
#undef ASSERT
#undef ASSERTinfo
#undef ASSERTeq
#undef ASSERTne
#include "unittest.h"

/*
 * print_custom_function -- Custom function to handle output
 *
 * This is called from the library to print text instead of output to stderr.
 */
static void
print_custom_function(const char *s)
{
	if (s) {
		OUT("CUSTOM_PRINT: %s", s);
	} else {
		OUT("CUSTOM_PRINT(NULL)");
	}
}

/*
 * vsnprintf_custom_function -- Custom vsnprintf implementation
 *
 * It modifies format by adding @@ in front of each conversion specification.
 */
static int
vsnprintf_custom_function(char *str, size_t size, const char *format,
		va_list ap)
{
	char format2[strlen(format) * 3];
	int i = 0;

	while (*format != '\0') {
		if (*format == '%') {
			format2[i++] = '@';
			format2[i++] = '@';
		}
		format2[i++] = *format++;
	}
	format2[i++] = '\0';

	return vsnprintf(str, size, format2, ap);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "traces_custom_function");

	if (argc != 2)
		FATAL("usage: %s [v|p]", argv[0]);

	out_set_print_func(print_custom_function);

	out_init(LOG_PREFIX, LOG_LEVEL_VAR, LOG_FILE_VAR,
			MAJOR_VERSION, MINOR_VERSION);

	switch (argv[1][0]) {
	case 'p': {
		LOG(0, "Log level NONE");
		LOG(1, "Log level ERROR");
		LOG(2, "Log level WARNING");
		LOG(3, "Log level INFO");
		LOG(4, "Log level DEBUG");
	}
		break;
	case 'v':
		out_set_vsnprintf_func(vsnprintf_custom_function);

		LOG(0, "no format");
		LOG(0, "pointer: %p", (void *)0x12345678);
		LOG(0, "string: %s", "Hello world!");
		LOG(0, "number: %u", 12345678);
		errno = EINVAL;
		LOG(0, "!error");
		break;
	default:
		FATAL("usage: %s [v|p]", argv[0]);
	}

	/* Cleanup */
	out_fini();

	DONE(NULL);
}
