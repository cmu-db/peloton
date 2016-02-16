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
 * blk_recovery.c -- unit test for pmemblk recovery
 *
 * usage: blk_recovery bsize file first_lba lba
 *
 */

#include "unittest.h"

#include <sys/param.h>

#include "util.h"
#include "blk.h"
#include "btt_layout.h"

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

sigjmp_buf Jmp;

/*
 * signal_handler -- called on SIGSEGV
 */
static void
signal_handler(int sig)
{
	OUT("signal: %s", strsignal(sig));

	siglongjmp(Jmp, 1);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "blk_recovery");

	if (argc != 5)
		FATAL("usage: %s bsize file first_lba lba", argv[0]);

	Bsize = strtoul(argv[1], NULL, 0);
	const char *path = argv[2];

	PMEMblkpool *handle;
	if ((handle = pmemblk_create(path, Bsize, 0,
			S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!%s: pmemblk_create", path);

	OUT("%s block size %zu usable blocks %zu",
			argv[1], Bsize, pmemblk_nblock(handle));

	/* write the first lba */
	off_t lba = strtoul(argv[3], NULL, 0);
	unsigned char buf[Bsize];

	construct(buf);
	if (pmemblk_write(handle, buf, lba) < 0)
		FATAL("!write     lba %zu", lba);

	OUT("write     lba %zu: %s", lba, ident(buf));

	/* reach into the layout and write-protect the map */
	struct btt_info *infop = (void *)((char *)handle +
		roundup(sizeof (struct pmemblk), BLK_FORMAT_DATA_ALIGN));

	char *mapaddr = (char *)infop + le32toh(infop->mapoff);
	char *flogaddr = (char *)infop + le32toh(infop->flogoff);

	OUT("write-protecting map, length %zu", (size_t)(flogaddr - mapaddr));
	MPROTECT(mapaddr, (size_t)(flogaddr - mapaddr), PROT_READ);

	/* arrange to catch SEGV */
	struct sigaction v;
	sigemptyset(&v.sa_mask);
	v.sa_flags = 0;
	v.sa_handler = signal_handler;
	SIGACTION(SIGSEGV, &v, NULL);

	/* map each file argument with the given map type */
	lba = strtoul(argv[4], NULL, 0);

	construct(buf);

	if (!sigsetjmp(Jmp, 1)) {
		if (pmemblk_write(handle, buf, lba) < 0)
			FATAL("!write     lba %zu", lba);
		else
			FATAL("write     lba %zu: %s", lba, ident(buf));
	}

	pmemblk_close(handle);

	int result = pmemblk_check(path, Bsize);
	if (result < 0)
		OUT("!%s: pmemblk_check", path);
	else if (result == 0)
		OUT("%s: pmemblk_check: not consistent", path);
	else
		OUT("%s: consistent", path);

	DONE(NULL);
}
