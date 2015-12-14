#include "pool.h"

void *
malloc_test(size_t size) {
	custom_allocs++;
	return malloc(size);
}

void
free_test(void *ptr) {
	custom_allocs--;
	free(ptr);
}

int
main(void)
{
	/*
	 * Initialize custom allocator who call malloc from jemalloc.
	 */
	if (nallocx(1, 0) == 0) {
		malloc_printf("Initialization error");
		return (test_status_fail);
	}

	je_pool_set_alloc_funcs(malloc_test, free_test);

	return test_not_init(POOL_TEST_CASES);
}
