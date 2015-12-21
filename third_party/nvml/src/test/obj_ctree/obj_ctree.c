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
 * obj_ctree.c -- unit test for crit-bit tree
 */
#include <stdint.h>
#include <pthread.h>

#include "ctree.h"
#include "unittest.h"

enum {
	TEST_NEW_DELETE	=	0,
	TEST_INSERT	=	100,
	TEST_REMOVE	=	200,
};

#define	TEST_VAL_A 1
#define	TEST_VAL_B 2
#define	TEST_VAL_C 3

FUNC_MOCK(malloc, void *, size_t size)
	FUNC_MOCK_RUN_RET_DEFAULT_REAL(malloc, size)
	FUNC_MOCK_RUN(TEST_INSERT + 0) /* leaf malloc */
	FUNC_MOCK_RUN(TEST_INSERT + 3) /* accessor malloc */
	FUNC_MOCK_RUN(TEST_NEW_DELETE + 0) { /* t malloc */
		return NULL;
	}
FUNC_MOCK_END

FUNC_MOCK(pthread_mutex_init, int,
	pthread_mutex_t *mutex, const pthread_mutexattr_t *attr)
	FUNC_MOCK_RUN_RET_DEFAULT_REAL(pthread_mutex_init, mutex, attr)
	FUNC_MOCK_RUN(TEST_NEW_DELETE + 0) {
		return -1;
	}
FUNC_MOCK_END

FUNC_MOCK(pthread_mutex_lock, int, pthread_mutex_t *mutex)
	FUNC_MOCK_RUN_RET_DEFAULT_REAL(pthread_mutex_lock, mutex)
	FUNC_MOCK_RUN(TEST_REMOVE + 0)
	FUNC_MOCK_RUN(TEST_INSERT + 1) {
		return -1;
	}
FUNC_MOCK_END

static void
test_ctree_new_delete_empty()
{
	struct ctree *t = NULL;

	FUNC_MOCK_RCOUNTER_SET(malloc, TEST_NEW_DELETE);
	FUNC_MOCK_RCOUNTER_SET(pthread_mutex_init, TEST_NEW_DELETE);
	FUNC_MOCK_RCOUNTER_SET(pthread_mutex_lock, TEST_NEW_DELETE);

	/* t Malloc fail */
	t = ctree_new();
	ASSERT(t == NULL);

	/* t->lock pthread_mutex_init fail */
	t = ctree_new();
	ASSERT(t == NULL);

	/* all OK and delete */
	t = ctree_new();
	ASSERT(t != NULL);

	ctree_delete(t);
}

static void
test_ctree_insert()
{
	struct ctree *t = ctree_new();
	ASSERT(t != NULL);

	FUNC_MOCK_RCOUNTER_SET(malloc, TEST_INSERT);
	FUNC_MOCK_RCOUNTER_SET(pthread_mutex_init, TEST_INSERT);
	FUNC_MOCK_RCOUNTER_SET(pthread_mutex_lock, TEST_INSERT);

	ASSERT(ctree_is_empty(t));

	/* pthread_mutex_lock fail */
	ASSERT(ctree_insert(t, TEST_VAL_A, 0) != 0);

	/* leaf Malloc fail */
	ASSERT(ctree_insert(t, TEST_VAL_A, 0) != 0);

	/* all OK root */
	ASSERT(ctree_insert(t, TEST_VAL_B, 0) == 0); /* insert +2 mallocs */

	/* accessor Malloc fail */
	ASSERT(ctree_insert(t, TEST_VAL_A, 0) != 0);

	/* insert duplicate */
	ASSERT(ctree_insert(t, TEST_VAL_B, 0) != 0);

	/* all OK second */
	ASSERT(ctree_insert(t, TEST_VAL_A, 0) == 0);

	ASSERT(!ctree_is_empty(t));

	ctree_delete(t);
}

static void
test_ctree_find()
{
	struct ctree *t = ctree_new();
	ASSERT(t != NULL);

	/* search empty tree */
	uint64_t k = TEST_VAL_A;
	ASSERT(ctree_find_le(t, &k) == 0);

	/* insert 2 valid elements */
	ASSERT(ctree_insert(t, TEST_VAL_A, TEST_VAL_A) == 0);
	ASSERT(ctree_insert(t, TEST_VAL_B, TEST_VAL_B) == 0);

	/* search for values */
	k = 0;
	ASSERT(ctree_find_le(t, &k) == 0);
	k = TEST_VAL_A;
	ASSERT(ctree_find_le(t, &k) == TEST_VAL_A);
	k = TEST_VAL_B;
	ASSERT(ctree_find_le(t, &k) == TEST_VAL_B);

	ctree_delete(t);
}

static void
test_ctree_remove()
{
	struct ctree *t = ctree_new();
	ASSERT(t != NULL);

	FUNC_MOCK_RCOUNTER_SET(malloc, TEST_REMOVE);
	FUNC_MOCK_RCOUNTER_SET(pthread_mutex_init, TEST_REMOVE);
	FUNC_MOCK_RCOUNTER_SET(pthread_mutex_lock, TEST_REMOVE);

	/* pthread_mutex_lock fail */
	ASSERT(ctree_remove(t, TEST_VAL_A, 0) == 0);

	/* remove from empty tree */
	ASSERT(ctree_remove(t, TEST_VAL_A, 0) == 0);

	/* insert 2 valid values */
	ASSERT(ctree_insert(t, TEST_VAL_A, 0) == 0);
	ASSERT(ctree_insert(t, TEST_VAL_B, 0) == 0);

	/* fail to remove equal greater */
	ASSERT(ctree_remove(t, TEST_VAL_C, 0) == 0);

	/* remove accessor */
	ASSERT(ctree_remove(t, TEST_VAL_A, 1) == TEST_VAL_A);

	/* remove root */
	ASSERT(ctree_remove(t, TEST_VAL_B, 1) == TEST_VAL_B);

	ctree_delete(t);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_ctree");

	test_ctree_new_delete_empty();
	test_ctree_insert();
	test_ctree_find();
	test_ctree_remove();

	DONE(NULL);
}
