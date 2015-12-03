/*
 * Copyright (c) 2014, Intel Corporation
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
 * printlog -- given a log file, print the entries
 *
 * Usage:
 *	printlog [-t] /path/to/pm-aware/file
 *
 * -t option means truncate the file after printing it.
 */

#include <stdio.h>
#include <fcntl.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <libpmemlog.h>

#include "logentry.h"

/*
 * printlog -- callback function called when walking the log
 */
int
printlog(const void *buf, size_t len, void *arg)
{
	const void *endp = buf + len;	/* first byte after log contents */

	/* for each entry in the log... */
	while (buf < endp) {
		struct logentry *headerp = (struct logentry *)buf;
		buf += sizeof (struct logentry);

		/* print the header */
		printf("Entry from pid: %ld\n", (long)headerp->pid);
		printf("       Created: %s", ctime(&headerp->timestamp));
		printf("      Contents:\n");

		/* print the log data itself */
		fwrite(buf, headerp->len, 1, stdout);
		buf += headerp->len;
	}

	return 0;
}

int
main(int argc, char *argv[])
{
	int opt;
	int tflag = 0;
	PMEMlogpool *plp;

	while ((opt = getopt(argc, argv, "t")) != -1)
		switch (opt) {
		case 't':
			tflag = 1;
			break;

		default:
			fprintf(stderr, "usage: %s [-t] file\n", argv[0]);
			exit(1);
		}

	if (optind >= argc) {
		fprintf(stderr, "usage: %s [-t] file\n", argv[0]);
		exit(1);
	}

	const char *path = argv[optind];

	if ((plp = pmemlog_open(path)) == NULL) {
		perror(path);
		exit(1);
	}

	/* the rest of the work happens in printlog() above */
	pmemlog_walk(plp, 0, printlog, NULL);

	if (tflag)
		pmemlog_rewind(plp);

	pmemlog_close(plp);
}
