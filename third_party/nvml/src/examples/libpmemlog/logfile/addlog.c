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
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY LOG OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * addlog -- given a log file, append a log entry
 *
 * Usage:
 *	fallocate -l 1G /path/to/pm-aware/file
 *	addlog /path/to/pm-aware/file "first line of entry" "second line"
 */

#include <sys/stat.h>
#include <stdio.h>
#include <fcntl.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <libpmemlog.h>

#include "logentry.h"

int
main(int argc, char *argv[])
{
	PMEMlogpool *plp;
	struct logentry header;
	struct iovec *iovp;
	struct iovec *next_iovp;
	int iovcnt;

	if (argc < 3) {
		fprintf(stderr, "usage: %s filename lines...\n", argv[0]);
		exit(1);
	}

	const char *path = argv[1];

	/* create the log in the given file, or open it if already created */
	if ((plp = pmemlog_create(path, 0, S_IWUSR | S_IRUSR)) == NULL &&
	    (plp = pmemlog_open(path)) == NULL) {
		perror(path);
		exit(1);
	}

	/* fill in the header */
	time(&header.timestamp);
	header.pid = getpid();

	/*
	 * Create an iov for pmemlog_appendv().  For each argument given,
	 * allocate two entries (one for the string, one for the newline
	 * appended to the string).  Allocate 1 additional entry for the
	 * header that gets prepended to the entry.
	 */
	iovcnt = (argc - 2) * 2 + 1;
	if ((iovp = malloc(sizeof (*iovp) * iovcnt)) == NULL) {
		perror("malloc");
		exit(1);
	}
	next_iovp = iovp;

	/* put the header into iov first */
	next_iovp->iov_base = &header;
	next_iovp->iov_len = sizeof (header);
	next_iovp++;

	/*
	 * Now put each arg in, following it with the string "\n".
	 * Calculate a total character count in header.len along the way.
	 */
	header.len = 0;
	for (int arg = 2; arg < argc; arg++) {
		/* add the string given */
		next_iovp->iov_base = argv[arg];
		next_iovp->iov_len = strlen(argv[arg]);
		header.len += next_iovp->iov_len;
		next_iovp++;

		/* add the newline */
		next_iovp->iov_base = "\n";
		next_iovp->iov_len = 1;
		header.len += 1;
		next_iovp++;
	}

	/* atomically add it all to the log */
	if (pmemlog_appendv(plp, iovp, iovcnt) < 0) {
		perror("pmemlog_appendv");
		free(iovp);
		exit(1);
	}

	free(iovp);

	pmemlog_close(plp);
}
