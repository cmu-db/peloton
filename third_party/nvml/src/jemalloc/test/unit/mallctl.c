#include "test/jemalloc_test.h"

TEST_BEGIN(test_mallctl_errors)
{
	uint64_t epoch;
	size_t sz;

	assert_d_eq(mallctl("no_such_name", NULL, NULL, NULL, 0), ENOENT,
	    "mallctl() should return ENOENT for non-existent names");

	assert_d_eq(mallctl("version", NULL, NULL, "0.0.0", strlen("0.0.0")),
	    EPERM, "mallctl() should return EPERM on attempt to write "
	    "read-only value");

	assert_d_eq(mallctl("epoch", NULL, NULL, &epoch, sizeof(epoch)-1),
	    EINVAL, "mallctl() should return EINVAL for input size mismatch");
	assert_d_eq(mallctl("epoch", NULL, NULL, &epoch, sizeof(epoch)+1),
	    EINVAL, "mallctl() should return EINVAL for input size mismatch");

	sz = sizeof(epoch)-1;
	assert_d_eq(mallctl("epoch", &epoch, &sz, NULL, 0), EINVAL,
	    "mallctl() should return EINVAL for output size mismatch");
	sz = sizeof(epoch)+1;
	assert_d_eq(mallctl("epoch", &epoch, &sz, NULL, 0), EINVAL,
	    "mallctl() should return EINVAL for output size mismatch");
}
TEST_END

TEST_BEGIN(test_mallctlnametomib_errors)
{
	size_t mib[1];
	size_t miblen;

	miblen = sizeof(mib)/sizeof(size_t);
	assert_d_eq(mallctlnametomib("no_such_name", mib, &miblen), ENOENT,
	    "mallctlnametomib() should return ENOENT for non-existent names");
}
TEST_END

TEST_BEGIN(test_mallctlbymib_errors)
{
	uint64_t epoch;
	size_t sz;
	size_t mib[1];
	size_t miblen;

	miblen = sizeof(mib)/sizeof(size_t);
	assert_d_eq(mallctlnametomib("version", mib, &miblen), 0,
	    "Unexpected mallctlnametomib() failure");

	assert_d_eq(mallctlbymib(mib, miblen, NULL, NULL, "0.0.0",
	    strlen("0.0.0")), EPERM, "mallctl() should return EPERM on "
	    "attempt to write read-only value");

	miblen = sizeof(mib)/sizeof(size_t);
	assert_d_eq(mallctlnametomib("epoch", mib, &miblen), 0,
	    "Unexpected mallctlnametomib() failure");

	assert_d_eq(mallctlbymib(mib, miblen, NULL, NULL, &epoch,
	    sizeof(epoch)-1), EINVAL,
	    "mallctlbymib() should return EINVAL for input size mismatch");
	assert_d_eq(mallctlbymib(mib, miblen, NULL, NULL, &epoch,
	    sizeof(epoch)+1), EINVAL,
	    "mallctlbymib() should return EINVAL for input size mismatch");

	sz = sizeof(epoch)-1;
	assert_d_eq(mallctlbymib(mib, miblen, &epoch, &sz, NULL, 0), EINVAL,
	    "mallctlbymib() should return EINVAL for output size mismatch");
	sz = sizeof(epoch)+1;
	assert_d_eq(mallctlbymib(mib, miblen, &epoch, &sz, NULL, 0), EINVAL,
	    "mallctlbymib() should return EINVAL for output size mismatch");
}
TEST_END

TEST_BEGIN(test_mallctl_read_write)
{
	uint64_t old_epoch, new_epoch;
	size_t sz = sizeof(old_epoch);

	/* Blind. */
	assert_d_eq(mallctl("epoch", NULL, NULL, NULL, 0), 0,
	    "Unexpected mallctl() failure");
	assert_zu_eq(sz, sizeof(old_epoch), "Unexpected output size");

	/* Read. */
	assert_d_eq(mallctl("epoch", &old_epoch, &sz, NULL, 0), 0,
	    "Unexpected mallctl() failure");
	assert_zu_eq(sz, sizeof(old_epoch), "Unexpected output size");

	/* Write. */
	assert_d_eq(mallctl("epoch", NULL, NULL, &new_epoch, sizeof(new_epoch)),
	    0, "Unexpected mallctl() failure");
	assert_zu_eq(sz, sizeof(old_epoch), "Unexpected output size");

	/* Read+write. */
	assert_d_eq(mallctl("epoch", &old_epoch, &sz, &new_epoch,
	    sizeof(new_epoch)), 0, "Unexpected mallctl() failure");
	assert_zu_eq(sz, sizeof(old_epoch), "Unexpected output size");
}
TEST_END

TEST_BEGIN(test_mallctlnametomib_short_mib)
{
	size_t mib[6];
	size_t miblen;
	void *mem;
	pool_t *pool;
	unsigned npools;
	size_t sz = sizeof(npools);

	mem = calloc(1, POOL_MINIMAL_SIZE);
	assert_ptr_ne(mem, NULL, "Unexpected calloc() failure");
	pool = je_pool_create(mem, POOL_MINIMAL_SIZE, 1);

	assert_ptr_ne((void*)pool, NULL, "Unexpected je_pool_create() failure");
	assert_d_eq(mallctl("pools.npools", &npools, &sz, NULL, 0), 0,
	    "Unexpected mallctl() failure");
	assert_u_eq(npools, 2, "Unexpected number of pools");

	miblen = 5;
	mib[5] = 42;
	assert_d_eq(mallctlnametomib("pool.1.arenas.bin.0.nregs", mib, &miblen), 0,
	    "Unexpected mallctlnametomib() failure");
	assert_zu_eq(miblen, 5, "Unexpected mib output length");
	assert_zu_eq(mib[5], 42,
	    "mallctlnametomib() wrote past the end of the input mib");

	je_pool_delete(pool);
	free(mem);
}
TEST_END

TEST_BEGIN(test_mallctl_config)
{

#define	TEST_MALLCTL_CONFIG(config) do {				\
	bool oldval;							\
	size_t sz = sizeof(oldval);					\
	assert_d_eq(mallctl("config."#config, &oldval, &sz, NULL, 0),	\
	    0, "Unexpected mallctl() failure");				\
	assert_b_eq(oldval, config_##config, "Incorrect config value");	\
	assert_zu_eq(sz, sizeof(oldval), "Unexpected output size");	\
} while (0)

	TEST_MALLCTL_CONFIG(debug);
	TEST_MALLCTL_CONFIG(fill);
	TEST_MALLCTL_CONFIG(lazy_lock);
	TEST_MALLCTL_CONFIG(munmap);
	TEST_MALLCTL_CONFIG(prof);
	TEST_MALLCTL_CONFIG(prof_libgcc);
	TEST_MALLCTL_CONFIG(prof_libunwind);
	TEST_MALLCTL_CONFIG(stats);
	TEST_MALLCTL_CONFIG(tcache);
	TEST_MALLCTL_CONFIG(tls);
	TEST_MALLCTL_CONFIG(utrace);
	TEST_MALLCTL_CONFIG(valgrind);
	TEST_MALLCTL_CONFIG(xmalloc);

#undef TEST_MALLCTL_CONFIG
}
TEST_END

TEST_BEGIN(test_mallctl_opt)
{
	bool config_always = true;

#define	TEST_MALLCTL_OPT(t, opt, config) do {				\
	t oldval;							\
	size_t sz = sizeof(oldval);					\
	int expected = config_##config ? 0 : ENOENT;			\
	int result = mallctl("opt."#opt, &oldval, &sz, NULL, 0);	\
	assert_d_eq(result, expected,					\
	    "Unexpected mallctl() result for opt."#opt);		\
	assert_zu_eq(sz, sizeof(oldval), "Unexpected output size");	\
} while (0)

	TEST_MALLCTL_OPT(bool, abort, always);
	TEST_MALLCTL_OPT(size_t, lg_chunk, always);
	TEST_MALLCTL_OPT(const char *, dss, always);
	TEST_MALLCTL_OPT(size_t, narenas, always);
	TEST_MALLCTL_OPT(ssize_t, lg_dirty_mult, always);
	TEST_MALLCTL_OPT(bool, stats_print, always);
	TEST_MALLCTL_OPT(bool, junk, fill);
	TEST_MALLCTL_OPT(size_t, quarantine, fill);
	TEST_MALLCTL_OPT(bool, redzone, fill);
	TEST_MALLCTL_OPT(bool, zero, fill);
	TEST_MALLCTL_OPT(bool, utrace, utrace);
	TEST_MALLCTL_OPT(bool, xmalloc, xmalloc);
	TEST_MALLCTL_OPT(bool, tcache, tcache);
	TEST_MALLCTL_OPT(size_t, lg_tcache_max, tcache);
	TEST_MALLCTL_OPT(bool, prof, prof);
	TEST_MALLCTL_OPT(const char *, prof_prefix, prof);
	TEST_MALLCTL_OPT(bool, prof_active, prof);
	TEST_MALLCTL_OPT(ssize_t, lg_prof_sample, prof);
	TEST_MALLCTL_OPT(bool, prof_accum, prof);
	TEST_MALLCTL_OPT(ssize_t, lg_prof_interval, prof);
	TEST_MALLCTL_OPT(bool, prof_gdump, prof);
	TEST_MALLCTL_OPT(bool, prof_final, prof);
	TEST_MALLCTL_OPT(bool, prof_leak, prof);

#undef TEST_MALLCTL_OPT
}
TEST_END


/*
 * create a couple of pools and check their size
 * using mib feature
 */
TEST_BEGIN(test_mallctl_with_multiple_pools)
{
#define NPOOLS 4
	pool_t *pools[NPOOLS];
	void *mem;
	unsigned npools;
	int i;
	size_t sz = sizeof(npools);
	size_t mib[4], miblen;

	mem = calloc(NPOOLS, POOL_MINIMAL_SIZE);
	assert_ptr_ne(mem, NULL, "Unexpected calloc() failure");

	for (i = 0; i < NPOOLS; ++i) {
		pools[i] = je_pool_create( mem + (i*POOL_MINIMAL_SIZE), POOL_MINIMAL_SIZE, 1);
		assert_ptr_ne( (void*)pools[i], NULL, "Unexpected je_pool_create() failure");
	}

	assert_d_eq(mallctl("pools.npools", &npools, &sz, NULL, 0), 0,
	    "Unexpected mallctl() failure");
	assert_u_eq(npools, NPOOLS+1, "Unexpected number of pools");

	miblen = 4;
	assert_d_eq(mallctlnametomib("pool.0.arenas.narenas", mib, &miblen), 0,
	    "Unexpected mallctlnametomib() failure");

	/*
	 * This loop does not use local variable pools.
	 * Moreover we ommit pool[0].
	 */
	for (i = 1; i <= NPOOLS; ++i) {
		unsigned narenas;
		mib[1] = i;
		sz = sizeof(narenas);
		assert_d_eq(mallctlbymib(mib, miblen, &narenas, &sz, NULL, 0),
		    0, "Unexpected mallctlbymib() failure");
	}

	for (i = 0; i < NPOOLS; ++i) {
		je_pool_delete( pools[i]);
	}
	free(mem);
#undef NPOOLS
}
TEST_END

TEST_BEGIN(test_manpage_example)
{
	unsigned nbins, i;
	size_t mib[6];
	size_t len, miblen;

	len = sizeof(nbins);
	assert_d_eq(mallctl("pool.0.arenas.nbins", &nbins, &len, NULL, 0), 0,
	    "Unexpected mallctl() failure");

	miblen = 6;
	assert_d_eq(mallctlnametomib("pool.0.arenas.bin.0.size", mib, &miblen), 0,
	    "Unexpected mallctlnametomib() failure");
	for (i = 0; i < nbins; i++) {
		size_t bin_size;

		mib[4] = i;
		len = sizeof(bin_size);
		assert_d_eq(mallctlbymib(mib, miblen, &bin_size, &len, NULL, 0),
		    0, "Unexpected mallctlbymib() failure");
		/* Do something with bin_size... */
	}
}
TEST_END

TEST_BEGIN(test_thread_arena)
{
	unsigned arena_old, arena_new, narenas;
	size_t sz = sizeof(unsigned);

	assert_d_eq(mallctl("pool.0.arenas.narenas", &narenas, &sz, NULL, 0), 0,
	    "Unexpected mallctl() failure");
	assert_u_eq(narenas, opt_narenas, "Number of arenas incorrect");
	arena_new = narenas - 1;
	assert_d_eq(mallctl("thread.pool.0.arena", &arena_old, &sz, &arena_new,
	    sizeof(unsigned)), 0, "Unexpected mallctl() failure");
	arena_new = 0;
	assert_d_eq(mallctl("thread.pool.0.arena", &arena_old, &sz, &arena_new,
	    sizeof(unsigned)), 0, "Unexpected mallctl() failure");
}
TEST_END

TEST_BEGIN(test_arena_i_purge)
{
	unsigned narenas;
	unsigned npools;
	size_t sz = sizeof(unsigned);
	size_t mib[5];
	size_t miblen = 5;
	void *mem;
	pool_t *pool;

	mem = calloc(1, POOL_MINIMAL_SIZE);
	assert_ptr_ne(mem, NULL, "Unexpected calloc() failure");
	pool = je_pool_create(mem, POOL_MINIMAL_SIZE, 1);

	assert_ptr_ne( (void*)pool, NULL, "Unexpected je_pool_create() failure");
	assert_d_eq(mallctl("pools.npools", &npools, &sz, NULL, 0), 0,
	    "Unexpected mallctl() failure");
	assert_u_eq(npools, 2, "Unexpected number of pools");

	assert_d_eq(mallctl("pool.1.arena.0.purge", NULL, NULL, NULL, 0), 0,
	    "Unexpected mallctl() failure");
	assert_d_eq(mallctl("pool.1.arenas.narenas", &narenas, &sz, NULL, 0), 0,
	    "Unexpected mallctl() failure");

	assert_d_eq(mallctlnametomib("pool.1.arena.0.purge", mib, &miblen), 0,
	    "Unexpected mallctlnametomib() failure");
	mib[3] = narenas;
	assert_d_eq(mallctlbymib(mib, miblen, NULL, NULL, NULL, 0), 0,
	    "Unexpected mallctlbymib() failure");

	je_pool_delete(pool);
	free(mem);
}
TEST_END

TEST_BEGIN(test_arena_i_dss)
{
	const char *dss_prec_old, *dss_prec_new;
	size_t sz = sizeof(dss_prec_old);
	size_t mib[5];
	size_t miblen;

	miblen = sizeof(mib)/sizeof(size_t);
	assert_d_eq(mallctlnametomib("pool.0.arena.0.dss", mib, &miblen), 0,
	    "Unexpected mallctlnametomib() error");

	dss_prec_new = "disabled";
	assert_d_eq(mallctlbymib(mib, miblen, &dss_prec_old, &sz, &dss_prec_new,
	    sizeof(dss_prec_new)), 0, "Unexpected mallctl() failure");
	assert_str_ne(dss_prec_old, "primary",
	    "Unexpected default for dss precedence");

	assert_d_eq(mallctlbymib(mib, miblen, &dss_prec_new, &sz, &dss_prec_old,
	    sizeof(dss_prec_old)), 0, "Unexpected mallctl() failure");

	mib[3] = narenas_total_get(pools[0]);
	dss_prec_new = "disabled";
	assert_d_eq(mallctlbymib(mib, miblen, &dss_prec_old, &sz, &dss_prec_new,
	    sizeof(dss_prec_new)), 0, "Unexpected mallctl() failure");
	assert_str_ne(dss_prec_old, "primary",
	    "Unexpected default for dss precedence");
}
TEST_END

TEST_BEGIN(test_arenas_initialized)
{
	unsigned narenas;
	size_t sz = sizeof(narenas);

	assert_d_eq(mallctl("pool.0.arenas.narenas", &narenas, &sz, NULL, 0), 0,
	    "Unexpected mallctl() failure");
	{
		VARIABLE_ARRAY(bool, initialized, narenas);

		sz = narenas * sizeof(bool);
		assert_d_eq(mallctl("pool.0.arenas.initialized", initialized, &sz,
		    NULL, 0), 0, "Unexpected mallctl() failure");
	}
}
TEST_END

TEST_BEGIN(test_arenas_constants)
{

#define	TEST_ARENAS_CONSTANT(t, name, expected) do {			\
	t name;								\
	size_t sz = sizeof(t);						\
	assert_d_eq(mallctl("pool.0.arenas."#name, &name, &sz, NULL, 0), 0,	\
	    "Unexpected mallctl() failure");				\
	assert_zu_eq(name, expected, "Incorrect "#name" size");		\
} while (0)

	TEST_ARENAS_CONSTANT(size_t, quantum, QUANTUM);
	TEST_ARENAS_CONSTANT(size_t, page, PAGE);
	TEST_ARENAS_CONSTANT(unsigned, nbins, NBINS);
	TEST_ARENAS_CONSTANT(size_t, nlruns, nlclasses);

#undef TEST_ARENAS_CONSTANT
}
TEST_END

TEST_BEGIN(test_arenas_bin_constants)
{

#define	TEST_ARENAS_BIN_CONSTANT(t, name, expected) do {		\
	t name;								\
	size_t sz = sizeof(t);						\
	assert_d_eq(mallctl("pool.0.arenas.bin.0."#name, &name, &sz, NULL, 0),	\
	    0, "Unexpected mallctl() failure");				\
	assert_zu_eq(name, expected, "Incorrect "#name" size");		\
} while (0)
	TEST_ARENAS_BIN_CONSTANT(size_t, size, arena_bin_info[0].reg_size);
	TEST_ARENAS_BIN_CONSTANT(uint32_t, nregs, arena_bin_info[0].nregs);
	TEST_ARENAS_BIN_CONSTANT(size_t, run_size, arena_bin_info[0].run_size);

#undef TEST_ARENAS_BIN_CONSTANT
}
TEST_END

TEST_BEGIN(test_arenas_lrun_constants)
{

#define	TEST_ARENAS_LRUN_CONSTANT(t, name, expected) do {		\
	t name;								\
	size_t sz = sizeof(t);						\
	assert_d_eq(mallctl("pool.0.arenas.lrun.0."#name, &name, &sz, NULL,	\
	    0), 0, "Unexpected mallctl() failure");			\
	assert_zu_eq(name, expected, "Incorrect "#name" size");		\
} while (0)
	TEST_ARENAS_LRUN_CONSTANT(size_t, size, (1 << LG_PAGE));

#undef TEST_ARENAS_LRUN_CONSTANT
}
TEST_END

/*
 * create a couple of pools and extend their arenas
 */
TEST_BEGIN(test_arenas_extend)
{
#define NPOOLS 4
	pool_t *pools[NPOOLS];
	void *mem;
	unsigned npools, narenas_before, arena, narenas_after;
	int i;
	size_t mib_narenas[4],
	       mib_extend[4],
	       miblen	= sizeof(mib_narenas),
	       sz	= sizeof(unsigned);

	mem = calloc(NPOOLS, POOL_MINIMAL_SIZE);
	assert_ptr_ne(mem, NULL, "Unexpected calloc() failure");

	for (i = 0; i < NPOOLS; ++i) {
		pools[i] = je_pool_create(mem + (i*POOL_MINIMAL_SIZE), POOL_MINIMAL_SIZE, 0);
		assert_ptr_ne((void *)pools[i], NULL, "Unexpected je_pool_create() failure");
	}

	assert_d_eq(mallctl("pools.npools", &npools, &sz, NULL, 0), 0,
	    "Unexpected mallctl() failure");
	assert_u_eq(npools, NPOOLS+1, "Unexpected number of pools");

	assert_d_eq(mallctlnametomib("pool.0.arenas.narenas", mib_narenas, &miblen), 0,
	    "Unexpected mallctlnametomib() failure");
	assert_d_eq(mallctlnametomib("pool.0.arenas.extend", mib_extend, &miblen), 0,
	    "Unexpected mallctlnametomib() failure");

	/*
	 * This loop does not use local variable pools.
	 * Moreover we ommit pool[0].
	 */
	for (i = 1; i <= NPOOLS; ++i) {
		mib_narenas[1] = i;
		mib_extend[1] = i;
		assert_d_eq(mallctlbymib(mib_narenas, miblen, &narenas_before, &sz, NULL, 0),
		    0, "Unexpected mallctlbymib() failure");
		assert_d_eq(mallctlbymib(mib_extend, miblen, &arena, &sz, NULL, 0),
		    0, "Unexpected mallctlbymib() failure");
		assert_d_eq(mallctlbymib(mib_narenas, miblen, &narenas_after, &sz, NULL, 0),
		    0, "Unexpected mallctlbymib() failure");

		assert_u_eq(narenas_before+1, narenas_after,
		    "Unexpected number of arenas before versus after extension");
		assert_u_eq(arena, narenas_after-1, "Unexpected arena index");
	}
	for (i = 0; i < NPOOLS; ++i) {
		je_pool_delete( pools[i]);
	}
	free(mem);
#undef NPOOLS
}
TEST_END

TEST_BEGIN(test_stats_arenas)
{

#define	TEST_STATS_ARENAS(t, name) do {					\
	t name;								\
	size_t sz = sizeof(t);						\
	assert_d_eq(mallctl("pool.0.stats.arenas.0."#name, &name, &sz, NULL,	\
	    0), 0, "Unexpected mallctl() failure");			\
} while (0)

	TEST_STATS_ARENAS(const char *, dss);
	TEST_STATS_ARENAS(unsigned, nthreads);
	TEST_STATS_ARENAS(size_t, pactive);
	TEST_STATS_ARENAS(size_t, pdirty);

#undef TEST_STATS_ARENAS
}
TEST_END

int
main(void)
{

	return (test(
	    test_mallctl_errors,
	    test_mallctlnametomib_errors,
	    test_mallctlbymib_errors,
	    test_mallctl_read_write,
	    test_mallctlnametomib_short_mib,
	    test_mallctl_config,
	    test_mallctl_opt,
	    test_mallctl_with_multiple_pools,
	    test_manpage_example,
	    test_thread_arena,
	    test_arena_i_purge,
	    test_arena_i_dss,
	    test_arenas_initialized,
	    test_arenas_constants,
	    test_arenas_bin_constants,
	    test_arenas_lrun_constants,
	    test_arenas_extend,
	    test_stats_arenas));
}
