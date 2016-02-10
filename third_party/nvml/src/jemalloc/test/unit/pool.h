#include "test/jemalloc_test.h"

#define TEST_POOL_SIZE (16L * 1024L * 1024L)
#define TEST_TOO_SMALL_POOL_SIZE (2L * 1024L * 1024L)
#define TEST_VALUE 123456
#define TEST_MALLOC_FREE_LOOPS 2
#define TEST_MALLOC_SIZE 1024
#define TEST_ALLOCS_SIZE (TEST_POOL_SIZE / 8)
#define TEST_BUFFOR_CMP_SIZE (4L * 1024L * 1024L)

static char mem_pool[TEST_POOL_SIZE];
static char mem_extend_ok[TEST_POOL_SIZE];
static void* allocs[TEST_ALLOCS_SIZE];

static int custom_allocs;

TEST_BEGIN(test_pool_create_errors) {
	pool_t *pool;
	memset(mem_pool, 1, TEST_POOL_SIZE);
	pool = pool_create(mem_pool, 0, 0);
	assert_ptr_null(pool, "pool_create() should return NULL for size 0");

	pool = pool_create(NULL, TEST_POOL_SIZE, 0);
	assert_ptr_null(pool, "pool_create() should return NULL for input addr NULL");
}
TEST_END

TEST_BEGIN(test_pool_create) {
	pool_t *pool;
	custom_allocs = 0;
	memset(mem_pool, 0, TEST_POOL_SIZE);
	pool = pool_create(mem_pool, TEST_POOL_SIZE, 1);
	assert_ptr_eq(pool, mem_pool, "pool_create() should return addr with valid input");
	pool_delete(pool);

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");
}
TEST_END

TEST_BEGIN(test_pool_malloc) {
	pool_t *pool;
	custom_allocs = 0;
	memset(mem_pool, 0, TEST_POOL_SIZE);
	pool = pool_create(mem_pool, TEST_POOL_SIZE, 1);

	int *test = pool_malloc(pool, sizeof(int));
	assert_ptr_not_null(test, "pool_malloc should return valid ptr");

	*test = TEST_VALUE;
	assert_x_eq(*test, TEST_VALUE, "ptr should be usable");

	assert_lu_gt((uintptr_t)test, (uintptr_t)mem_pool,
		"pool_malloc() should return pointer to memory from pool");
	assert_lu_lt((uintptr_t)test, (uintptr_t)mem_pool+TEST_POOL_SIZE,
		"pool_malloc() should return pointer to memory from pool");

	pool_free(pool, test);

	pool_delete(pool);

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");
}
TEST_END

TEST_BEGIN(test_pool_free) {
	pool_t *pool;
	int i, j, s = 0, prev_s = 0;
	int allocs = TEST_POOL_SIZE/TEST_MALLOC_SIZE;
	void *arr[allocs];
	custom_allocs = 0;
	memset(mem_pool, 0, TEST_POOL_SIZE);
	pool = pool_create(mem_pool, TEST_POOL_SIZE, 1);

	for (i = 0; i < TEST_MALLOC_FREE_LOOPS; ++i) {
		for (j = 0; j < allocs; ++j) {
			arr[j] = pool_malloc(pool, TEST_MALLOC_SIZE);
			if (arr[j] != NULL) {
				s++;
			}
		}
		for (j = 0; j < allocs; ++j) {
			if (arr[j] != NULL) {
				pool_free(pool, arr[j]);
			}
		}
		if (prev_s != 0) {
			assert_x_eq(s, prev_s,
				"pool_free() should record back used chunks");
		}

		prev_s = s;
		s = 0;
	}

	pool_delete(pool);

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");
}
TEST_END

TEST_BEGIN(test_pool_calloc) {
	pool_t *pool;
	custom_allocs = 0;
	memset(mem_pool, 1, TEST_POOL_SIZE);
	pool = pool_create(mem_pool, TEST_POOL_SIZE, 0);

	int *test = pool_calloc(pool, 1, sizeof(int));
	assert_ptr_not_null(test, "pool_calloc should return valid ptr");

	assert_x_eq(*test, 0, "pool_calloc should return zeroed memory");

	pool_free(pool, test);

	pool_delete(pool);

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");
}
TEST_END

TEST_BEGIN(test_pool_realloc) {
	pool_t *pool;
	custom_allocs = 0;
	memset(mem_pool, 0, TEST_POOL_SIZE);
	pool = pool_create(mem_pool, TEST_POOL_SIZE, 1);

	int *test = pool_ralloc(pool, NULL, sizeof(int));
	assert_ptr_not_null(test, "pool_ralloc with NULL addr should return valid ptr");

	int *test2 = pool_ralloc(pool, test, sizeof(int)*2);
	assert_ptr_not_null(test, "pool_ralloc should return valid ptr");
	test2[0] = TEST_VALUE;
	test2[1] = TEST_VALUE;

	assert_x_eq(test[1], TEST_VALUE, "ptr should be usable");

	pool_free(pool, test2);

	pool_delete(pool);

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");
}
TEST_END

TEST_BEGIN(test_pool_aligned_alloc) {
	pool_t *pool;
	custom_allocs = 0;
	memset(mem_pool, 0, TEST_POOL_SIZE);
	pool = pool_create(mem_pool, TEST_POOL_SIZE, 1);

	int *test = pool_aligned_alloc(pool, 1024, 1024);
	assert_ptr_not_null(test, "pool_aligned_alloc should return valid ptr");
	assert_x_eq(((uintptr_t)(test) & 1023), 0, "ptr should be aligned");

	assert_lu_gt((uintptr_t)test, (uintptr_t)mem_pool,
		"pool_aligned_alloc() should return pointer to memory from pool");
	assert_lu_lt((uintptr_t)test, (uintptr_t)mem_pool+TEST_POOL_SIZE,
		"pool_aligned_alloc() should return pointer to memory from pool");

	*test = TEST_VALUE;
	assert_x_eq(*test, TEST_VALUE, "ptr should be usable");

	pool_free(pool, test);

	pool_delete(pool);

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");
}
TEST_END

TEST_BEGIN(test_pool_reuse_pool) {
	pool_t *pool;
	size_t  pool_num = 0;
	custom_allocs = 0;

	/* create and destroy pool multiple times */
	for (; pool_num<100; ++pool_num) {
		pool = pool_create(mem_pool, TEST_POOL_SIZE, 0);
		assert_ptr_not_null(pool, "Can not create pool!!!");
		if (pool == NULL) {
			break;
		}

		void *prev = NULL;
		size_t i = 0;

		/* allocate memory from pool */
		for (; i<100; ++i) {
			void **next = pool_malloc(pool, sizeof (void *));

			assert_lu_gt((uintptr_t)next, (uintptr_t)mem_pool,
				"pool_malloc() should return pointer to memory from pool");
			assert_lu_lt((uintptr_t)next, (uintptr_t)mem_pool+TEST_POOL_SIZE,
				"pool_malloc() should return pointer to memory from pool");

			*next = prev;
			prev = next;
		}

		/* free all allocated memory from pool */
		while (prev != NULL) {
			void **act = prev;
			prev = *act;
			pool_free(pool, act);
		}
		pool_delete(pool);
	}

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");
}
TEST_END

TEST_BEGIN(test_pool_check_memory) {
	pool_t *pool;
	size_t pool_size = POOL_MINIMAL_SIZE;
	assert_lu_lt(POOL_MINIMAL_SIZE, TEST_POOL_SIZE, "Too small pool size");

	size_t object_size;
	size_t size_allocated;
	size_t i;
	size_t j;

	for (object_size = 8; object_size <= TEST_BUFFOR_CMP_SIZE ; object_size *= 2) {
		custom_allocs = 0;
		pool = pool_create(mem_pool, pool_size, 0);
		assert_ptr_not_null(pool, "Can not create pool!!!");
		size_allocated = 0;
		memset(allocs, 0, TEST_ALLOCS_SIZE);

		for (i = 0; i < TEST_ALLOCS_SIZE;++i) {
			allocs[i] = pool_malloc(pool, object_size);
			if (allocs[i] == NULL) {
				/* out of memory in pool */
				break;
			}
			assert_lu_gt((uintptr_t)allocs[i], (uintptr_t)mem_pool,
					"pool_malloc() should return pointer to memory from pool");
			assert_lu_lt((uintptr_t)allocs[i], (uintptr_t)mem_pool+pool_size,
				"pool_malloc() should return pointer to memory from pool");

			size_allocated += object_size;

			/* fill each allocation with a unique value */
			memset(allocs[i], (char)i, object_size);
		}

		assert_ptr_not_null(allocs[0], "pool_malloc should return valid ptr");
		assert_lu_lt(i + 1, TEST_ALLOCS_SIZE, "All memory should be used");

		/* check for unexpected modifications of prepare data */
		for (i = 0; i < TEST_ALLOCS_SIZE && allocs[i] != NULL; ++i) {
			char *buffer = allocs[i];
			for (j = 0; j < object_size; ++j)
				if (buffer[j] != (char)i) {
					assert_true(0, "Content of data object was modified unexpectedly"
						" for object size: %zu, id: %zu", object_size, j);
					break;
			}
		}

		pool_delete(pool);

		assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");
	}

}
TEST_END

TEST_BEGIN(test_pool_use_all_memory) {
	pool_t *pool;
	size_t size = 0;
	size_t pool_size = POOL_MINIMAL_SIZE;
	assert_lu_lt(POOL_MINIMAL_SIZE, TEST_POOL_SIZE, "Too small pool size");
	custom_allocs = 0;
	pool = pool_create(mem_pool, pool_size, 0);
	assert_ptr_not_null(pool, "Can not create pool!!!");

	void *prev = NULL;
	for (;;) {
		void **next = pool_malloc(pool, sizeof (void *));
		if (next == NULL) {
			/* Out of memory in pool, test end */
			break;
		}
		size += sizeof (void *);

		assert_ptr_not_null(next, "pool_malloc should return valid ptr");

		assert_lu_gt((uintptr_t)next, (uintptr_t)mem_pool,
				"pool_malloc() should return pointer to memory from pool");
		assert_lu_lt((uintptr_t)next, (uintptr_t)mem_pool+pool_size,
			"pool_malloc() should return pointer to memory from pool");

		*next = prev;
		assert_x_eq((uintptr_t)(*next), (uintptr_t)(prev), "ptr should be usable");
		prev = next;

	}

	assert_lu_gt(size, 0, "Can not alloc any memory from pool");

	/* Free all allocated memory from pool */
	while (prev != NULL) {
		void **act = prev;
		prev = *act;
		pool_free(pool, act);
	}

	pool_delete(pool);

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");
}
TEST_END

TEST_BEGIN(test_pool_extend_errors) {
	pool_t *pool;
	custom_allocs = 0;
	memset(mem_pool, 0, TEST_POOL_SIZE);
	pool = pool_create(mem_pool, TEST_POOL_SIZE, 1);

	memset(mem_extend_ok, 0, TEST_TOO_SMALL_POOL_SIZE);
	size_t usable_size = pool_extend(pool, mem_extend_ok, TEST_TOO_SMALL_POOL_SIZE, 0);

	assert_zu_eq(usable_size, 0, "pool_extend() should return 0"
		" when provided with memory size smaller then chunksize");

	pool_delete(pool);

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");
}
TEST_END

TEST_BEGIN(test_pool_extend) {
	pool_t *pool;
	custom_allocs = 0;
	memset(mem_pool, 0, TEST_POOL_SIZE);
	pool = pool_create(mem_pool, TEST_POOL_SIZE, 1);

	memset(mem_extend_ok, 0, TEST_POOL_SIZE);
	size_t usable_size = pool_extend(pool, mem_extend_ok, TEST_POOL_SIZE, 0);

	assert_zu_ne(usable_size, 0, "pool_extend() should return value"
		" after alignment when provided with enough memory");

	pool_delete(pool);

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");
}
TEST_END

TEST_BEGIN(test_pool_extend_after_out_of_memory) {
	pool_t *pool;
	custom_allocs = 0;
	memset(mem_pool, 0, TEST_POOL_SIZE);
	pool = pool_create(mem_pool, TEST_POOL_SIZE, 1);

	/* use the all memory from pool and from base allocator */
	while (pool_malloc(pool, sizeof (void *)));
	pool->base_next_addr = pool->base_past_addr;

	memset(mem_extend_ok, 0, TEST_POOL_SIZE);
	size_t usable_size = pool_extend(pool, mem_extend_ok, TEST_POOL_SIZE, 0);

	assert_zu_ne(usable_size, 0, "pool_extend() should return value"
		" after alignment when provided with enough memory");

	pool_delete(pool);

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");
}
TEST_END

/*
 * print_jemalloc_messages -- custom print function, for jemalloc
 */
static void
print_jemalloc_messages(void* ignore, const char *s)
{

}

TEST_BEGIN(test_pool_check_extend) {
	je_malloc_message = print_jemalloc_messages;
	pool_t *pool;
	custom_allocs = 0;

	pool = pool_create(mem_pool, TEST_POOL_SIZE, 0);
	pool_malloc(pool, 100);
	assert_d_eq(je_pool_check(pool), 1, "je_pool_check() return error");
	pool_delete(pool);
	assert_d_ne(je_pool_check(pool), 1, "je_pool_check() not return error");

	pool = pool_create(mem_pool, TEST_POOL_SIZE, 0);
	assert_d_eq(je_pool_check(pool), 1, "je_pool_check() return error");
	size_t size_extend = pool_extend(pool, mem_extend_ok, TEST_POOL_SIZE, 1);
	assert_zu_ne(size_extend, 0, "pool_extend() should add some free space");
	assert_d_eq(je_pool_check(pool), 1, "je_pool_check() return error");
	pool_malloc(pool, 100);
	pool_delete(pool);
	assert_d_ne(je_pool_check(pool), 1, "je_pool_check() not return error");

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");

	je_malloc_message = NULL;
}
TEST_END

TEST_BEGIN(test_pool_check_memory_out_of_range) {
	je_malloc_message = print_jemalloc_messages;
	pool_t *pool;
	custom_allocs = 0;

	pool = pool_create(mem_pool, TEST_POOL_SIZE, 0);
	assert_d_eq(je_pool_check(pool), 1, "je_pool_check() return error");

	void *usable_addr = (void *)CHUNK_CEILING((uintptr_t)mem_extend_ok);
	size_t usable_size = (TEST_POOL_SIZE - (uintptr_t)(usable_addr -
			(void *)mem_extend_ok)) & ~chunksize_mask;

	chunk_record(pool,
			&pool->chunks_szad_mmap, &pool->chunks_ad_mmap,
			usable_addr, usable_size, 0);

	assert_d_ne(je_pool_check(pool), 1, "je_pool_check() not return error");

	pool_delete(pool);
	assert_d_ne(je_pool_check(pool), 1, "je_pool_check() return error");

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");

	je_malloc_message = NULL;
}
TEST_END

TEST_BEGIN(test_pool_check_memory_overlap) {
	je_malloc_message = print_jemalloc_messages;
	pool_t *pool;
	pool_t *pool2;
	custom_allocs = 0;

	memset(mem_pool, 0, TEST_POOL_SIZE);
	pool = pool_create(mem_pool, TEST_POOL_SIZE, 1);
	size_t size_extend = pool_extend(pool, mem_extend_ok, TEST_POOL_SIZE, 1);
	assert_zu_ne(size_extend, 0, "pool_extend() should add some free space");
	assert_d_eq(je_pool_check(pool), 1, "je_pool_check() return error");

	/* create another pool in the same memory region */
	pool2 = pool_create(mem_extend_ok, TEST_POOL_SIZE, 0);
	assert_d_ne(je_pool_check(pool), 1, "je_pool_check() not return error");
	assert_d_ne(je_pool_check(pool2), 1, "je_pool_check() not return error");
	pool_delete(pool2);
	pool_delete(pool);

	assert_d_eq(custom_allocs, 0, "memory leak when using custom allocator");

	je_malloc_message = NULL;
}
TEST_END


#define	POOL_TEST_CASES\
	test_pool_create_errors,	\
	test_pool_create,	\
	test_pool_malloc,	\
	test_pool_free,	\
	test_pool_calloc,	\
	test_pool_realloc,	\
	test_pool_aligned_alloc,	\
	test_pool_reuse_pool,	\
	test_pool_check_memory,	\
	test_pool_use_all_memory,	\
	test_pool_extend_errors,	\
	test_pool_extend,	\
	test_pool_extend_after_out_of_memory,	\
	test_pool_check_extend,	\
	test_pool_check_memory_out_of_range,	\
	test_pool_check_memory_overlap

