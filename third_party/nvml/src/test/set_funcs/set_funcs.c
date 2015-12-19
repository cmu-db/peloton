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
 * set_funcs.c -- unit test for pmem*_set_funcs()
 */
#include "unittest.h"

#define	OBJ 0
#define	BLK 1
#define	LOG 2
#define	VMEM_ 3

#define	VMEM_POOLS 4

static struct counters {
	int mallocs;
	int frees;
	int reallocs;
	int strdups;
} cnt[4];

static void *
obj_malloc(size_t size)
{
	cnt[OBJ].mallocs++;
	return malloc(size);
}

static void
obj_free(void *ptr)
{
	if (ptr)
		cnt[OBJ].frees++;
	free(ptr);
}

static void *
obj_realloc(void *ptr, size_t size)
{
	cnt[OBJ].reallocs++;
	return realloc(ptr, size);
}

static char *
obj_strdup(const char *s)
{
	cnt[OBJ].strdups++;
	return strdup(s);
}


static void *
blk_malloc(size_t size)
{
	cnt[BLK].mallocs++;
	return malloc(size);
}

static void
blk_free(void *ptr)
{
	if (ptr)
		cnt[BLK].frees++;
	free(ptr);
}

static void *
blk_realloc(void *ptr, size_t size)
{
	cnt[BLK].reallocs++;
	return realloc(ptr, size);
}

static char *
blk_strdup(const char *s)
{
	cnt[BLK].strdups++;
	return strdup(s);
}

static void *
log_malloc(size_t size)
{
	cnt[LOG].mallocs++;
	return malloc(size);
}

static void
log_free(void *ptr)
{
	if (ptr)
		cnt[LOG].frees++;
	free(ptr);
}

static void *
log_realloc(void *ptr, size_t size)
{
	cnt[LOG].reallocs++;
	return realloc(ptr, size);
}

static char *
log_strdup(const char *s)
{
	cnt[LOG].strdups++;
	return strdup(s);
}

static void *
_vmem_malloc(size_t size)
{
	cnt[VMEM_].mallocs++;
	return malloc(size);
}

static void
_vmem_free(void *ptr)
{
	if (ptr)
		cnt[VMEM_].frees++;
	free(ptr);
}

static void *
_vmem_realloc(void *ptr, size_t size)
{
	cnt[VMEM_].reallocs++;
	return realloc(ptr, size);
}

static char *
_vmem_strdup(const char *s)
{
	cnt[VMEM_].strdups++;
	return strdup(s);
}

static void
test_obj(const char *path)
{
	memset(cnt, 0, sizeof (cnt));

	PMEMobjpool *pop =
			pmemobj_create(path, NULL, PMEMOBJ_MIN_POOL, 0600);

	PMEMoid oid;

	if (pmemobj_alloc(pop, &oid, 10, 0, NULL, NULL))
		FATAL("!alloc");

	if (pmemobj_realloc(pop, &oid, 100, 0))
		FATAL("!realloc");

	pmemobj_free(&oid);

	pmemobj_close(pop);

	OUT("obj_mallocs: %d", cnt[OBJ].mallocs);
	OUT("obj_frees: %d", cnt[OBJ].frees);
	OUT("obj_reallocs: %d", cnt[OBJ].reallocs);
	OUT("obj_strdups: %d", cnt[OBJ].strdups);

	if (cnt[OBJ].mallocs == 0 || cnt[OBJ].frees == 0)
		FATAL("OBJ mallocs: %d, frees: %d", cnt[OBJ].mallocs,
				cnt[OBJ].frees);
	for (int i = 0; i < 4; ++i) {
		if (i == OBJ)
			continue;
		if (cnt[i].mallocs || cnt[i].frees)
			FATAL("OBJ allocation used %d functions", i);
	}
	if (cnt[OBJ].mallocs + cnt[OBJ].strdups != cnt[OBJ].frees)
		FATAL("OBJ memory leak");

	unlink(path);
}

static void
test_blk(const char *path)
{
	memset(cnt, 0, sizeof (cnt));

	PMEMblkpool *blk = pmemblk_create(path, 512, PMEMBLK_MIN_POOL, 0600);

	pmemblk_close(blk);


	OUT("blk_mallocs: %d", cnt[BLK].mallocs);
	OUT("blk_frees: %d", cnt[BLK].frees);
	OUT("blk_reallocs: %d", cnt[BLK].reallocs);
	OUT("blk_strdups: %d", cnt[BLK].strdups);

	if (cnt[BLK].mallocs == 0 || cnt[BLK].frees == 0)
		FATAL("BLK mallocs: %d, frees: %d", cnt[BLK].mallocs,
				cnt[BLK].frees);
	for (int i = 0; i < 4; ++i) {
		if (i == BLK)
			continue;
		if (cnt[i].mallocs || cnt[i].frees)
			FATAL("BLK allocation used %d functions", i);
	}
	if (cnt[BLK].mallocs + cnt[BLK].strdups != cnt[BLK].frees)
		FATAL("BLK memory leak");

	unlink(path);
}

static void
test_log(const char *path)
{
	memset(cnt, 0, sizeof (cnt));

	PMEMlogpool *log = pmemlog_create(path, PMEMLOG_MIN_POOL, 0600);

	pmemlog_close(log);


	OUT("log_mallocs: %d", cnt[LOG].mallocs);
	OUT("log_frees: %d", cnt[LOG].frees);
	OUT("log_reallocs: %d", cnt[LOG].reallocs);
	OUT("log_strdups: %d", cnt[LOG].strdups);

	if (cnt[LOG].mallocs == 0 || cnt[LOG].frees == 0)
		FATAL("LOG mallocs: %d, frees: %d", cnt[LOG].mallocs,
				cnt[LOG].frees);
	for (int i = 0; i < 4; ++i) {
		if (i == LOG)
			continue;
		if (cnt[i].mallocs || cnt[i].frees)
			FATAL("LOG allocation used %d functions", i);
	}
	if (cnt[LOG].mallocs + cnt[LOG].strdups != cnt[LOG].frees)
		FATAL("LOG memory leak");

	unlink(path);
}

static void
test_vmem(const char *dir)
{
	memset(cnt, 0, sizeof (cnt));

	VMEM *v[VMEM_POOLS];
	void *ptr[VMEM_POOLS];

	for (int i = 0; i < VMEM_POOLS; i++) {
		v[i] = vmem_create(dir, VMEM_MIN_POOL);
		ptr[i] = vmem_malloc(v[i], 64);
		vmem_free(v[i], ptr[i]);
	}

	for (int i = 0; i < VMEM_POOLS; i++)
		vmem_delete(v[i]);

	OUT("vmem_mallocs: %d", cnt[VMEM_].mallocs);
	OUT("vmem_frees: %d", cnt[VMEM_].frees);
	OUT("vmem_reallocs: %d", cnt[VMEM_].reallocs);
	OUT("vmem_strdups: %d", cnt[VMEM_].strdups);

	if (cnt[VMEM_].mallocs == 0 && cnt[VMEM_].frees == 0)
		FATAL("VMEM mallocs: %d, frees: %d", cnt[VMEM_].mallocs,
				cnt[VMEM_].frees);
	for (int i = 0; i < 4; ++i) {
		if (i == VMEM_)
			continue;
		if (cnt[i].mallocs || cnt[i].frees)
			FATAL("VMEM allocation used %d functions", i);
	}
	if (cnt[VMEM_].mallocs + cnt[VMEM_].strdups > cnt[VMEM_].frees + 4)
		FATAL("VMEM memory leak");
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "set_funcs");

	if (argc < 3)
		FATAL("usage: %s file dir", argv[0]);

	pmemobj_set_funcs(obj_malloc, obj_free, obj_realloc, obj_strdup);
	pmemblk_set_funcs(blk_malloc, blk_free, blk_realloc, blk_strdup);
	pmemlog_set_funcs(log_malloc, log_free, log_realloc, log_strdup);
	vmem_set_funcs(_vmem_malloc, _vmem_free, _vmem_realloc, _vmem_strdup,
			NULL);

	test_obj(argv[1]);
	test_blk(argv[1]);
	test_log(argv[1]);
	test_vmem(argv[2]);

	DONE(NULL);
}
