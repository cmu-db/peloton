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
 * asset_list -- list all assets in an assetdb file
 *
 * Usage:
 *	asset_list /path/to/pm-aware/file
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>
#include <libpmemblk.h>

#include "asset.h"

int
main(int argc, char *argv[])
{
	PMEMblkpool *pbp;
	int assetid;
	size_t nelements;
	struct asset asset;

	if (argc < 2) {
		fprintf(stderr, "usage: %s assetdb\n", argv[0]);
		exit(1);
	}

	const char *path = argv[1];

	/* open an array of atomically writable elements */
	if ((pbp = pmemblk_open(path, sizeof (struct asset))) == NULL) {
		perror(path);
		exit(1);
	}

	/* how many elements do we have? */
	nelements = pmemblk_nblock(pbp);

	/* print out all the elements that contain assets data */
	for (assetid = 0; assetid < nelements; ++assetid) {
		if (pmemblk_read(pbp, &asset, (off_t)assetid) < 0) {
			perror("pmemblk_read");
			exit(1);
		}

		if ((asset.state != ASSET_FREE) &&
			(asset.state != ASSET_CHECKED_OUT)) {
			break;
		}

		printf("Asset ID: %d\n", assetid);
		if (asset.state == ASSET_FREE)
			printf("   State: Free\n");
		else {
			printf("   State: Checked out\n");
			printf("    User: %s\n", asset.user);
			printf("    Time: %s", ctime(&asset.time));
		}
		printf("    Name: %s\n", asset.name);
	}

	pmemblk_close(pbp);
}
