#include "pool.h"

static char buff_alloc[4*1024];
static char *buff_ptr = buff_alloc;

void *
malloc_test(size_t size) {
	custom_allocs++;
	void *ret = buff_ptr;
	buff_ptr = buff_ptr + size;
	return ret;
}

void
free_test(void *ptr) {
	custom_allocs--;
	if(custom_allocs == 0) {
		buff_ptr = buff_alloc;
	}
}

int
main(void)
{
	je_pool_set_alloc_funcs(malloc_test, free_test);

	return test_not_init(POOL_TEST_CASES);
}
