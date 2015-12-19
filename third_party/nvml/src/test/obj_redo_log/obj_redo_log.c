/*
 * Copyright (c) 2015, Intel Corporation
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
 * obj_redo_log.c -- unit test for redo log
 *
 * usage: obj_redo_log <redo_log_size> [sfrePR][:offset[:value]]
 *
 * s:<index>:<offset>:<value> - store <value> at <offset>
 * f:<index>:<offset>:<value> - store last <value> at <offset>
 * F:<index>                  - set <index> entry as the last one
 * r:<offset>                 - read at <offset>
 * e:<index>                  - read redo log entry at <index>
 * P                          - process redo log
 * R                          - recovery
 * C                          - check  consistency of redo log
 *
 * <offset> and <value> must be in hex
 * <index> must be in dec
 *
 */
#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

#include "libpmemobj.h"
#include "util.h"
#include "lane.h"
#include "redo.h"
#include "list.h"
#include "obj.h"
#include "valgrind_internal.h"

#include "unittest.h"

#define	FATAL_USAGE()	FATAL("usage: obj_redo_log <fname> <redo_log_size> "\
		"[sfFrePRC][<index>:<offset>:<value>]\n")

#define	PMEMOBJ_POOL_HDR_SIZE	8192

static void
pmem_drain_nop(void)
{
}

/*
 * obj_persist -- pmemobj version of pmem_persist w/o replication
 */
static void
obj_persist(PMEMobjpool *pop, void *addr, size_t len)
{
	pop->persist_local(addr, len);
}

/*
 * obj_flush -- pmemobj version of pmem_flush w/o replication
 */
static void
obj_flush(PMEMobjpool *pop, void *addr, size_t len)
{
	pop->flush_local(addr, len);
}

/*
 * obj_drain -- pmemobj version of pmem_drain w/o replication
 */
static void
obj_drain(PMEMobjpool *pop)
{
	pop->drain_local();
}


static PMEMobjpool *
pmemobj_open_mock(const char *fname)
{
	int fd = open(fname, O_RDWR);
	if (fd == -1) {
		OUT("!%s: open", fname);

		return NULL;
	}

	struct stat stbuf;
	if (fstat(fd, &stbuf) < 0) {
		OUT("!fstat");
		(void) close(fd);

		return NULL;
	}

	ASSERT(stbuf.st_size > PMEMOBJ_POOL_HDR_SIZE);

	void *addr = pmem_map(fd);
	if (!addr) {
		OUT("!%s: pmem_map", fname);
		(void) close(fd);

		return NULL;
	}

	close(fd);

	PMEMobjpool *pop = (PMEMobjpool *)addr;
	VALGRIND_REMOVE_PMEM_MAPPING((char *)addr + sizeof (pop->hdr), 4096);
	pop->addr = addr;
	pop->size = stbuf.st_size;
	pop->is_pmem = pmem_is_pmem(addr, stbuf.st_size);
	pop->rdonly = 0;

	if (pop->is_pmem) {
		pop->persist_local = pmem_persist;
		pop->flush_local = pmem_flush;
		pop->drain_local = pmem_drain;
	} else {
		pop->persist_local = (persist_local_fn)pmem_msync;
		pop->flush_local = (persist_local_fn)pmem_msync;
		pop->drain_local = pmem_drain_nop;
	}

	pop->persist = obj_persist;
	pop->flush = obj_flush;
	pop->drain = obj_drain;

	return pop;
}

static void
pmemobj_close_mock(PMEMobjpool *pop)
{
	munmap(pop, pop->size);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_redo_log");
	util_init();

	if (argc < 4)
		FATAL_USAGE();

	PMEMobjpool *pop = pmemobj_open_mock(argv[1]);
	ASSERTne(pop, NULL);

	ASSERTeq(util_is_zeroed((char *)pop->addr + PMEMOBJ_POOL_HDR_SIZE,
			pop->size - PMEMOBJ_POOL_HDR_SIZE), 1);

	char *end = NULL;
	errno = 0;
	size_t redo_size = strtoul(argv[2], &end, 0);
	if (errno || !end || *end != '\0')
		FATAL_USAGE();

	ASSERT(pop->size >= redo_size * sizeof (struct redo_log));

	struct redo_log *redo = (struct redo_log *)pop->addr;

	uint64_t offset;
	uint64_t value;
	int i;
	int ret;
	size_t index;
	for (i = 3; i < argc; i++) {
		char *arg = argv[i];
		ASSERTne(arg, NULL);

		switch (arg[0]) {
		case 's':
			if (sscanf(arg, "s:%ld:0x%lx:0x%lx",
					&index, &offset, &value) != 3)
				FATAL_USAGE();
			OUT("s:%ld:0x%08lx:0x%08lx", index, offset, value);
			redo_log_store(pop, redo, index, offset, value);
			break;
		case 'f':
			if (sscanf(arg, "f:%ld:0x%lx:0x%lx",
					&index, &offset, &value) != 3)
				FATAL_USAGE();
			OUT("f:%ld:0x%08lx:0x%08lx", index, offset, value);
			redo_log_store_last(pop, redo, index, offset,
					value);
			break;
		case 'F':
			if (sscanf(arg, "F:%ld", &index) != 1)
				FATAL_USAGE();
			OUT("F:%ld", index);
			redo_log_set_last(pop, redo, index);
			break;
		case 'r':
			if (sscanf(arg, "r:0x%lx", &offset) != 1)
				FATAL_USAGE();

			uint64_t *valp = (uint64_t *)((uintptr_t)pop->addr
					+ offset);
			OUT("r:0x%08lx:0x%08lx", offset, *valp);
			break;
		case 'e':
			if (sscanf(arg, "e:%ld", &index) != 1)
				FATAL_USAGE();

			struct redo_log *entry = redo + index;

			int flag = (entry->offset & REDO_FINISH_FLAG) ? 1 : 0;
			offset = entry->offset & REDO_FLAG_MASK;
			value = entry->value;

			OUT("e:%ld:0x%08lx:%d:0x%08lx", index, offset,
					flag, value);
			break;
		case 'P':
			redo_log_process(pop, redo, redo_size);
			OUT("P");
			break;
		case 'R':
			redo_log_recover(pop, redo, redo_size);
			OUT("R");
			break;
		case 'C':
			ret = redo_log_check(pop, redo, redo_size);
			OUT("C:%d", ret);
			break;
		default:
			FATAL_USAGE();
		}
	}

	pmemobj_close_mock(pop);

	DONE(NULL);
}
