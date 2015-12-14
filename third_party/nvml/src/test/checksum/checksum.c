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
 * checksum.c -- unit test for library internal checksum routine
 *
 * usage: checksum files...
 */

#include <endian.h>
#include "unittest.h"

#include "util.h"

/*
 * fletcher64 -- compute a Fletcher64 checksum
 *
 * Gold standard implementation used to compare to the
 * util_checksum() being unit tested.
 */
static uint64_t
fletcher64(void *addr, size_t len)
{
	ASSERT(len % 4 == 0);
	uint32_t *p32 = addr;
	uint32_t *p32end = (uint32_t *)((char *)addr + len);
	uint32_t lo32 = 0;
	uint32_t hi32 = 0;

	while (p32 < p32end) {
		lo32 += le32toh(*p32);
		p32++;
		hi32 += lo32;
	}

	return htole64((uint64_t)hi32 << 32 | lo32);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "checksum");

	if (argc < 2)
		FATAL("usage: %s files...", argv[0]);

	for (int arg = 1; arg < argc; arg++) {
		int fd = OPEN(argv[arg], O_RDONLY);

		struct stat stbuf;
		FSTAT(fd, &stbuf);

		void *addr =
			MMAP(0, stbuf.st_size, PROT_READ|PROT_WRITE,
					MAP_PRIVATE, fd, 0);

		close(fd);

		uint64_t *ptr = addr;

		/*
		 * Loop through, selecting successive locations
		 * where the checksum lives in this block, and
		 * let util_checksum() insert it so it can be
		 * verified against the gold standard fletcher64
		 * routine in this file.
		 */
		while ((char *)(ptr + 1) < (char *)addr + stbuf.st_size) {
			/* save whatever was at *ptr */
			uint64_t oldval = *ptr;

			/* mess with it */
			*ptr = htole64(0x123);

			/*
			 * calculate a checksum and have it installed
			 */
			util_checksum(addr, stbuf.st_size, ptr, 1);

			uint64_t csum = *ptr;

			/*
			 * verify inserted checksum checks out
			 */
			ASSERT(util_checksum(addr, stbuf.st_size, ptr, 0));

			/* put a zero where the checksum was installed */
			*ptr = 0;

			/* calculate a checksum */
			uint64_t gold_csum = fletcher64(addr, stbuf.st_size);

			/* put the old value back */
			*ptr = oldval;

			/*
			 * verify checksum now fails
			 */
			ASSERT(!util_checksum(addr, stbuf.st_size, ptr, 0));

			/*
			 * verify the checksum matched the gold version
			 */
			ASSERTeq(csum, gold_csum);

			OUT("%s:%lu 0x%lx", argv[arg],
				(char *)ptr - (char *)addr, csum);

			ptr++;
		}
	}

	DONE(NULL);
}
