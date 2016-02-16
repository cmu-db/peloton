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
 * blk_rw.c -- unit test for pmemblk_read/write/set_zero/set_error
 *
 * usage: blk_rw bsize file func operation:lba...
 *
 * func is 'c' or 'o' (create or open)
 * operations are 'r' or 'w' or 'z' or 'e'
 *
 */

#include "unittest.h"

size_t Bsize;

/*
 * construct -- build a buffer for writing
 */
static void
construct(unsigned char *buf)
{
	static int ord = 1;

	for (int i = 0; i < Bsize; i++)
		buf[i] = ord;

	ord++;

	if (ord > 255)
		ord = 1;
}

/*
 * ident -- identify what a buffer holds
 */
static char *
ident(unsigned char *buf)
{
	static char descr[100];
	unsigned val = *buf;

	for (int i = 1; i < Bsize; i++)
		if (buf[i] != val) {
			sprintf(descr, "{%u} TORN at byte %d", val, i);
			return descr;
		}

	sprintf(descr, "{%u}", val);
	return descr;
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "blk_rw");

	if (argc < 5)
		FATAL("usage: %s bsize file func op:lba...", argv[0]);

	Bsize = strtoul(argv[1], NULL, 0);

	const char *path = argv[2];

	PMEMblkpool *handle;
	switch (*argv[3]) {
		case 'c':
			handle = pmemblk_create(path, Bsize, 0,
					S_IWUSR | S_IRUSR);
			if (handle == NULL)
				FATAL("!%s: pmemblk_create", path);
			break;
		case 'o':
			handle = pmemblk_open(path, Bsize);
			if (handle == NULL)
				FATAL("!%s: pmemblk_open", path);
			break;
	}

	OUT("%s block size %zu usable blocks %zu",
			argv[1], Bsize, pmemblk_nblock(handle));

	/* map each file argument with the given map type */
	for (int arg = 4; arg < argc; arg++) {
		if (strchr("rwze", argv[arg][0]) == NULL || argv[arg][1] != ':')
			FATAL("op must be r: or w: or z: or e:");
		off_t lba = strtol(&argv[arg][2], NULL, 0);

		unsigned char buf[Bsize];

		switch (argv[arg][0]) {
		case 'r':
			if (pmemblk_read(handle, buf, lba) < 0)
				OUT("!read      lba %jd", lba);
			else
				OUT("read      lba %jd: %s", lba, ident(buf));
			break;

		case 'w':
			construct(buf);
			if (pmemblk_write(handle, buf, lba) < 0)
				OUT("!write     lba %jd", lba);
			else
				OUT("write     lba %jd: %s", lba, ident(buf));
			break;

		case 'z':
			if (pmemblk_set_zero(handle, lba) < 0)
				OUT("!set_zero  lba %jd", lba);
			else
				OUT("set_zero  lba %jd", lba);
			break;

		case 'e':
			if (pmemblk_set_error(handle, lba) < 0)
				OUT("!set_error lba %jd", lba);
			else
				OUT("set_error lba %jd", lba);
			break;
		}
	}

	pmemblk_close(handle);

	int result = pmemblk_check(path, Bsize);
	if (result < 0)
		OUT("!%s: pmemblk_check", path);
	else if (result == 0)
		OUT("%s: pmemblk_check: not consistent", path);

	DONE(NULL);
}
