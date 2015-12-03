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
 * obj_cuckoo.c -- unit test for cuckoo hash table
 */

#include <errno.h>

#include "unittest.h"
#include "cuckoo.h"
#include "libpmemobj.h"
#include "util.h"

#define	TEST_INSERTS 100
#define	TEST_VAL(x) ((void *)((uintptr_t)(x)))

FUNC_MOCK(malloc, void *, size_t size)
	FUNC_MOCK_RUN(1) /* internal out_err malloc */
	FUNC_MOCK_RUN_RET_DEFAULT_REAL(malloc, size)
	FUNC_MOCK_RUN(2) /* tab malloc */
	FUNC_MOCK_RUN(0) /* cuckoo malloc */
		return NULL;
FUNC_MOCK_END

static void
test_cuckoo_new_delete()
{
	struct cuckoo *c = NULL;

	/* cuckoo malloc fail */
	c = cuckoo_new();
	ASSERT(c == NULL);

	/* tab malloc fail */
	c = cuckoo_new();
	ASSERT(c == NULL);

	/* all ok */
	c = cuckoo_new();
	ASSERT(c != NULL);

	cuckoo_delete(c);
}

static void
test_insert_get_remove()
{
	struct cuckoo *c = cuckoo_new();
	ASSERT(c != NULL);

	for (int i = 0; i < TEST_INSERTS; ++i)
		ASSERT(cuckoo_insert(c, i, TEST_VAL(i)) == 0);

	for (int i = 0; i < TEST_INSERTS; ++i)
		ASSERT(cuckoo_get(c, i) == TEST_VAL(i));

	for (int i = 0; i < TEST_INSERTS; ++i)
		ASSERT(cuckoo_remove(c, i) == TEST_VAL(i));

	for (int i = 0; i < TEST_INSERTS; ++i)
		ASSERT(cuckoo_remove(c, i) == NULL);

	for (int i = 0; i < TEST_INSERTS; ++i)
		ASSERT(cuckoo_get(c, i) == NULL);

	cuckoo_delete(c);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_cuckoo");

	test_cuckoo_new_delete();
	test_insert_get_remove();

	DONE(NULL);
}
