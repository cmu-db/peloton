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
 * asset_load -- given pre-allocated assetdb file, load it up with assets
 *
 * Usage:
 *	fallocate -l 1G /path/to/pm-aware/file
 *	asset_load /path/to/pm-aware/file asset-file
 *
 * The asset-file should contain the names of the assets, one per line.
 */

#include <sys/stat.h>
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
	FILE *fp;
	size_t len;
	PMEMblkpool *pbp;
	int assetid = 0;
	size_t nelements;
	char *line = NULL;

	if (argc < 3) {
		fprintf(stderr, "usage: %s assetdb assetlist\n", argv[0]);
		exit(1);
	}

	const char *path_pool = argv[1];
	const char *path_list = argv[2];

	/* create pmemblk pool in existing (but as yet unmodified) file */
	if ((pbp = pmemblk_create(path_pool,
			sizeof (struct asset), 0, S_IWUSR | S_IRUSR)) == NULL) {
		perror(path_pool);
		exit(1);
	}

	nelements = pmemblk_nblock(pbp);

	if ((fp = fopen(path_list, "r")) == NULL) {
		perror(path_list);
		exit(1);
	}

	/*
	 * Read in all the assets from the assetfile and put them in the
	 * array, if a name of the asset is longer than ASSET_NAME_SIZE_MAX,
	 * truncate it.
	 */
	while (getline(&line, &len, fp) != -1) {
		struct asset asset;

		if (assetid >= nelements) {
			fprintf(stderr, "%s: too many assets to fit in %s "
					"(only %d assets loaded)\n",
					path_list, path_pool, assetid);
			exit(1);
		}

		memset(&asset, '\0', sizeof (asset));
		asset.state = ASSET_FREE;
		strncpy(asset.name, line, ASSET_NAME_MAX - 1);
		asset.name[ASSET_NAME_MAX - 1] = '\0';

		if (pmemblk_write(pbp, &asset, (off_t)assetid) < 0) {
			perror("pmemblk_write");
			exit(1);
		}

		assetid++;
	}

	free(line);
	fclose(fp);

	pmemblk_close(pbp);
}
