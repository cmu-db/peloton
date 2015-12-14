#define	JEMALLOC_C_
#include "jemalloc/internal/jemalloc_internal.h"

/******************************************************************************/
/* Data. */
malloc_tsd_data(, arenas, tsd_pool_t, TSD_POOL_INITIALIZER)
malloc_tsd_data(, thread_allocated, thread_allocated_t,
    THREAD_ALLOCATED_INITIALIZER)

/* Runtime configuration options. */
const char	*je_malloc_conf;
bool	opt_abort =
#ifdef JEMALLOC_DEBUG
    true
#else
    false
#endif
    ;
bool	opt_junk =
#if (defined(JEMALLOC_DEBUG) && defined(JEMALLOC_FILL))
    true
#else
    false
#endif
    ;
size_t	opt_quarantine = ZU(0);
bool	opt_redzone = false;
bool	opt_utrace = false;
bool	opt_xmalloc = false;
bool	opt_zero = false;
size_t	opt_narenas = 0;

/* Initialized to true if the process is running inside Valgrind. */
bool	in_valgrind;


unsigned	npools_cnt;	/* actual number of pools */
unsigned	npools; 	/* size of the pools[] array */
unsigned	ncpus;

pool_t		**pools;
pool_t		base_pool;
unsigned	pool_seqno = 0;
bool		pools_shared_data_initialized;

/*
 * Custom malloc() and free() for shared data and for data needed to
 * initialize pool. If not defined functions then base_pool will be
 * created for allocations from RAM.
 */
void	*(*je_base_malloc)(size_t);
void	(*je_base_free)(void *);

/* Set to true once the allocator has been initialized. */
static bool		malloc_initialized = false;
static bool		base_pool_initialized = false;

#ifdef JEMALLOC_THREADED_INIT
/* Used to let the initializing thread recursively allocate. */
#  define NO_INITIALIZER	((unsigned long)0)
#  define INITIALIZER		pthread_self()
#  define IS_INITIALIZER	(malloc_initializer == pthread_self())
static pthread_t		malloc_initializer = NO_INITIALIZER;
#else
#  define NO_INITIALIZER	false
#  define INITIALIZER		true
#  define IS_INITIALIZER	malloc_initializer
static bool			malloc_initializer = NO_INITIALIZER;
#endif

/* Used to avoid initialization races. */
#ifdef _WIN32
static malloc_mutex_t	init_lock;

JEMALLOC_ATTR(constructor)
static void WINAPI
_init_init_lock(void)
{

	malloc_mutex_init(&init_lock);
}

#ifdef _MSC_VER
#  pragma section(".CRT$XCU", read)
JEMALLOC_SECTION(".CRT$XCU") JEMALLOC_ATTR(used)
static const void (WINAPI *init_init_lock)(void) = _init_init_lock;
#endif

#else
static malloc_mutex_t	init_lock = MALLOC_MUTEX_INITIALIZER;
#endif

typedef struct {
	void	*p;	/* Input pointer (as in realloc(p, s)). */
	size_t	s;	/* Request size. */
	void	*r;	/* Result pointer. */
} malloc_utrace_t;

#ifdef JEMALLOC_UTRACE
#  define UTRACE(a, b, c) do {						\
	if (opt_utrace) {						\
		int utrace_serrno = errno;				\
		malloc_utrace_t ut;					\
		ut.p = (a);						\
		ut.s = (b);						\
		ut.r = (c);						\
		utrace(&ut, sizeof(ut));				\
		errno = utrace_serrno;					\
	}								\
} while (0)
#else
#  define UTRACE(a, b, c)
#endif

/* data structures for callbacks used in je_pool_check() to browse trees */
typedef struct {
	pool_memory_range_node_t *list;
	size_t size;
	int error;
} check_data_cb_t;

/******************************************************************************/
/*
 * Function prototypes for static functions that are referenced prior to
 * definition.
 */

static bool	malloc_init_hard(void);
static bool	malloc_init_base_pool(void);
static void	*base_malloc_default(size_t size);
static void	base_free_default(void *ptr);

/******************************************************************************/
/*
 * Begin miscellaneous support functions.
 */

/* Create a new arena and insert it into the arenas array at index ind. */
arena_t *
arenas_extend(pool_t *pool, unsigned ind)
{
	arena_t *ret;

	ret = (arena_t *)base_alloc(pool, sizeof(arena_t));
	if (ret != NULL && arena_new(pool, ret, ind) == false) {
		pool->arenas[ind] = ret;
		return (ret);
	}
	/* Only reached if there is an OOM error. */

	/*
	 * OOM here is quite inconvenient to propagate, since dealing with it
	 * would require a check for failure in the fast path.  Instead, punt
	 * by using arenas[0].  In practice, this is an extremely unlikely
	 * failure.
	 */
	malloc_write("<jemalloc>: Error initializing arena\n");
	if (opt_abort)
		abort();

	return (pool->arenas[0]);
}

/* Slow path, called only by choose_arena(). */
arena_t *
choose_arena_hard(pool_t *pool)
{
	arena_t *ret;
	tsd_pool_t *tsd;

	if (pool->narenas_auto > 1) {
		unsigned i, choose, first_null;

		choose = 0;
		first_null = pool->narenas_auto;
		malloc_rwlock_wrlock(&pool->arenas_lock);
		assert(pool->arenas[0] != NULL);
		for (i = 1; i < pool->narenas_auto; i++) {
			if (pool->arenas[i] != NULL) {
				/*
				 * Choose the first arena that has the lowest
				 * number of threads assigned to it.
				 */
				if (pool->arenas[i]->nthreads <
				    pool->arenas[choose]->nthreads)
					choose = i;
			} else if (first_null == pool->narenas_auto) {
				/*
				 * Record the index of the first uninitialized
				 * arena, in case all extant arenas are in use.
				 *
				 * NB: It is possible for there to be
				 * discontinuities in terms of initialized
				 * versus uninitialized arenas, due to the
				 * "thread.arena" mallctl.
				 */
				first_null = i;
			}
		}

		if (pool->arenas[choose]->nthreads == 0
		    || first_null == pool->narenas_auto) {
			/*
			 * Use an unloaded arena, or the least loaded arena if
			 * all arenas are already initialized.
			 */
			ret = pool->arenas[choose];
		} else {
			/* Initialize a new arena. */
			ret = arenas_extend(pool, first_null);
		}
		ret->nthreads++;
		malloc_rwlock_unlock(&pool->arenas_lock);
	} else {
		ret = pool->arenas[0];
		malloc_rwlock_wrlock(&pool->arenas_lock);
		ret->nthreads++;
		malloc_rwlock_unlock(&pool->arenas_lock);
	}

	tsd = arenas_tsd_get();
	tsd->seqno[pool->pool_id] = pool->seqno;
	tsd->arenas[pool->pool_id] = ret;


	return (ret);
}

static void
stats_print_atexit(void)
{

	if (config_tcache && config_stats) {
		unsigned narenas, i, j;
		pool_t *pool;

		/*
		 * Merge stats from extant threads.  This is racy, since
		 * individual threads do not lock when recording tcache stats
		 * events.  As a consequence, the final stats may be slightly
		 * out of date by the time they are reported, if other threads
		 * continue to allocate.
		 */
		malloc_mutex_lock(&pools_lock);
		for (i = 0; i < npools; i++) {
			pool = pools[i];
			if (pool != NULL) {
				for (j = 0, narenas = narenas_total_get(pool); j < narenas; j++) {
					arena_t *arena = pool->arenas[j];
					if (arena != NULL) {
						tcache_t *tcache;

						/*
						 * tcache_stats_merge() locks bins, so if any
						 * code is introduced that acquires both arena
						 * and bin locks in the opposite order,
						 * deadlocks may result.
						 */
						malloc_mutex_lock(&arena->lock);
						ql_foreach(tcache, &arena->tcache_ql, link) {
							tcache_stats_merge(tcache, arena);
						}
						malloc_mutex_unlock(&arena->lock);
					}
				}
			}
		}
		malloc_mutex_unlock(&pools_lock);
	}
	je_malloc_stats_print(NULL, NULL, NULL);
}

/*
 * End miscellaneous support functions.
 */
/******************************************************************************/
/*
 * Begin initialization functions.
 */

static unsigned
malloc_ncpus(void)
{
	long result;

#ifdef _WIN32
	SYSTEM_INFO si;
	GetSystemInfo(&si);
	result = si.dwNumberOfProcessors;
#else
	result = sysconf(_SC_NPROCESSORS_ONLN);
#endif
	return ((result == -1) ? 1 : (unsigned)result);
}

bool
arenas_tsd_extend(tsd_pool_t *tsd, unsigned len)
{
	assert(len < POOLS_MAX);

	/* round up the new length to the nearest power of 2... */
	size_t npools = 1 << (32 - __builtin_clz(len + 1));

	/* ... but not less than */
	if (npools < POOLS_MIN)
		npools = POOLS_MIN;

	unsigned *tseqno = je_base_malloc(npools * sizeof (unsigned));
	if (tseqno == NULL)
		return (true);

	if (tsd->seqno != NULL)
		memcpy(tseqno, tsd->seqno, tsd->npools * sizeof (unsigned));
	memset(&tseqno[tsd->npools], 0, (npools - tsd->npools) * sizeof (unsigned));

	arena_t **tarenas = je_base_malloc(npools * sizeof (arena_t *));
	if (tarenas == NULL) {
		je_base_free(tseqno);
		return (true);
	}

	if (tsd->arenas != NULL)
		memcpy(tarenas, tsd->arenas, tsd->npools * sizeof (arena_t *));
	memset(&tarenas[tsd->npools], 0, (npools - tsd->npools) * sizeof (arena_t *));

	je_base_free(tsd->seqno);
	tsd->seqno = tseqno;
	je_base_free(tsd->arenas);
	tsd->arenas = tarenas;

	tsd->npools = npools;

	return (false);
}

void
arenas_cleanup(void *arg)
{
	unsigned i;
	pool_t *pool;
	tsd_pool_t *tsd = arg;

	malloc_mutex_lock(&pools_lock);
	for (i = 0; i < tsd->npools; i++) {
		pool = pools[i];
		if (pool != NULL) {
			if (pool->seqno == tsd->seqno[i] && tsd->arenas[i] != NULL) {
				malloc_rwlock_wrlock(&pool->arenas_lock);
				tsd->arenas[i]->nthreads--;
				malloc_rwlock_unlock(&pool->arenas_lock);
			}
		}
	}

	je_base_free(tsd->seqno);
	je_base_free(tsd->arenas);
	tsd->npools = 0;

	malloc_mutex_unlock(&pools_lock);

}

JEMALLOC_ALWAYS_INLINE_C void
malloc_thread_init(void)
{
	if (config_fill && opt_quarantine && je_base_malloc == base_malloc_default) {
		/* create pool base and call quarantine_alloc_hook() inside */
		malloc_init_base_pool();
	}
}

JEMALLOC_ALWAYS_INLINE_C bool
malloc_init(void)
{

	if (malloc_initialized == false && malloc_init_hard())
		return (true);

	return (false);
}

static bool
malloc_init_base_pool(void)
{

	malloc_mutex_lock(&pool_base_lock);

	if (base_pool_initialized) {
		/*
		 * Another thread initialized the base pool before this one
		 * acquired pools_lock.
		 */
		malloc_mutex_unlock(&pool_base_lock);
		return (false);
	}

	if (malloc_init()) {
		malloc_mutex_unlock(&pool_base_lock);
		return (true);
	}

	if (pool_new(&base_pool, 0)) {
		malloc_mutex_unlock(&pool_base_lock);
		return (true);
	}

	pools = base_calloc(&base_pool, sizeof(pool_t *), POOLS_MIN);
	if (pools == NULL) {
		malloc_mutex_unlock(&pool_base_lock);
		return (true);
	}

	pools[0] = &base_pool;
	pools[0]->seqno = ++pool_seqno;
	npools_cnt++;
	npools = POOLS_MIN;

	base_pool_initialized = true;
	malloc_mutex_unlock(&pool_base_lock);

	/*
	 * TSD initialization can't be safely done as a side effect of
	 * deallocation, because it is possible for a thread to do nothing but
	 * deallocate its TLS data via free(), in which case writing to TLS
	 * would cause write-after-free memory corruption.  The quarantine
	 * facility *only* gets used as a side effect of deallocation, so make
	 * a best effort attempt at initializing its TSD by hooking all
	 * allocation events.
	 */
	if (config_fill && opt_quarantine)
		quarantine_alloc_hook();

	return (false);
}

static bool
malloc_conf_next(char const **opts_p, char const **k_p, size_t *klen_p,
    char const **v_p, size_t *vlen_p)
{
	bool accept;
	const char *opts = *opts_p;

	*k_p = opts;

	for (accept = false; accept == false;) {
		switch (*opts) {
		case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
		case 'G': case 'H': case 'I': case 'J': case 'K': case 'L':
		case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R':
		case 'S': case 'T': case 'U': case 'V': case 'W': case 'X':
		case 'Y': case 'Z':
		case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
		case 'g': case 'h': case 'i': case 'j': case 'k': case 'l':
		case 'm': case 'n': case 'o': case 'p': case 'q': case 'r':
		case 's': case 't': case 'u': case 'v': case 'w': case 'x':
		case 'y': case 'z':
		case '0': case '1': case '2': case '3': case '4': case '5':
		case '6': case '7': case '8': case '9':
		case '_':
			opts++;
			break;
		case ':':
			opts++;
			*klen_p = (uintptr_t)opts - 1 - (uintptr_t)*k_p;
			*v_p = opts;
			accept = true;
			break;
		case '\0':
			if (opts != *opts_p) {
				malloc_write("<jemalloc>: Conf string ends "
				    "with key\n");
			}
			return (true);
		default:
			malloc_write("<jemalloc>: Malformed conf string\n");
			return (true);
		}
	}

	for (accept = false; accept == false;) {
		switch (*opts) {
		case ',':
			opts++;
			/*
			 * Look ahead one character here, because the next time
			 * this function is called, it will assume that end of
			 * input has been cleanly reached if no input remains,
			 * but we have optimistically already consumed the
			 * comma if one exists.
			 */
			if (*opts == '\0') {
				malloc_write("<jemalloc>: Conf string ends "
				    "with comma\n");
			}
			*vlen_p = (uintptr_t)opts - 1 - (uintptr_t)*v_p;
			accept = true;
			break;
		case '\0':
			*vlen_p = (uintptr_t)opts - (uintptr_t)*v_p;
			accept = true;
			break;
		default:
			opts++;
			break;
		}
	}

	*opts_p = opts;
	return (false);
}

static void
malloc_conf_error(const char *msg, const char *k, size_t klen, const char *v,
    size_t vlen)
{

	malloc_printf("<jemalloc>: %s: %.*s:%.*s\n", msg, (int)klen, k,
	    (int)vlen, v);
}

static void
malloc_conf_init(void)
{
	unsigned i;
	char buf[PATH_MAX + 1];
	const char *opts, *k, *v;
	size_t klen, vlen;

	/*
	 * Automatically configure valgrind before processing options.  The
	 * valgrind option remains in jemalloc 3.x for compatibility reasons.
	 */
	if (config_valgrind) {
		in_valgrind = (RUNNING_ON_VALGRIND != 0) ? true : false;
		if (config_fill && in_valgrind) {
			opt_junk = false;
			assert(opt_zero == false);
			opt_quarantine = JEMALLOC_VALGRIND_QUARANTINE_DEFAULT;
			opt_redzone = true;
		}
		if (config_tcache && in_valgrind)
			opt_tcache = false;
	}

	for (i = 0; i < 3; i++) {
		/* Get runtime configuration. */
		switch (i) {
		case 0:
			if (je_malloc_conf != NULL) {
				/*
				 * Use options that were compiled into the
				 * program.
				 */
				opts = je_malloc_conf;
			} else {
				/* No configuration specified. */
				buf[0] = '\0';
				opts = buf;
			}
			break;
		case 1: {
			int linklen = 0;
#ifndef _WIN32
			int saved_errno = errno;
			const char *linkname =
#  ifdef JEMALLOC_PREFIX
			    "/etc/"JEMALLOC_PREFIX"malloc.conf"
#  else
			    "/etc/malloc.conf"
#  endif
			    ;

			/*
			 * Try to use the contents of the "/etc/malloc.conf"
			 * symbolic link's name.
			 */
			linklen = readlink(linkname, buf, sizeof(buf) - 1);
			if (linklen == -1) {
				/* No configuration specified. */
				linklen = 0;
				/* restore errno */
				set_errno(saved_errno);
			}
#endif
			buf[linklen] = '\0';
			opts = buf;
			break;
		} case 2: {
			const char *envname =
#ifdef JEMALLOC_PREFIX
			    JEMALLOC_CPREFIX"MALLOC_CONF"
#else
			    "MALLOC_CONF"
#endif
			    ;

			if ((opts = getenv(envname)) != NULL) {
				/*
				 * Do nothing; opts is already initialized to
				 * the value of the MALLOC_CONF environment
				 * variable.
				 */
			} else {
				/* No configuration specified. */
				buf[0] = '\0';
				opts = buf;
			}
			break;
		} default:
			not_reached();
			buf[0] = '\0';
			opts = buf;
		}

		while (*opts != '\0' && malloc_conf_next(&opts, &k, &klen, &v,
		    &vlen) == false) {
#define	CONF_MATCH(n)							\
	(sizeof(n)-1 == klen && strncmp(n, k, klen) == 0)
#define	CONF_HANDLE_BOOL(o, n, cont)					\
			if (CONF_MATCH(n)) {				\
				if (strncmp("true", v, vlen) == 0 &&	\
				    vlen == sizeof("true")-1)		\
					o = true;			\
				else if (strncmp("false", v, vlen) ==	\
				    0 && vlen == sizeof("false")-1)	\
					o = false;			\
				else {					\
					malloc_conf_error(		\
					    "Invalid conf value",	\
					    k, klen, v, vlen);		\
				}					\
				if (cont)				\
					continue;			\
			}
#define	CONF_HANDLE_SIZE_T(o, n, min, max, clip)			\
			if (CONF_MATCH(n)) {				\
				uintmax_t um;				\
				char *end;				\
									\
				set_errno(0);				\
				um = malloc_strtoumax(v, &end, 0);	\
				if (get_errno() != 0 || (uintptr_t)end -\
				    (uintptr_t)v != vlen) {		\
					malloc_conf_error(		\
					    "Invalid conf value",	\
					    k, klen, v, vlen);		\
				} else if (clip) {			\
					if (min != 0 && um < min)	\
						o = min;		\
					else if (um > max)		\
						o = max;		\
					else				\
						o = um;			\
				} else {				\
					if ((min != 0 && um < min) ||	\
					    um > max) {			\
						malloc_conf_error(	\
						    "Out-of-range "	\
						    "conf value",	\
						    k, klen, v, vlen);	\
					} else				\
						o = um;			\
				}					\
				continue;				\
			}
#define	CONF_HANDLE_SSIZE_T(o, n, min, max)				\
			if (CONF_MATCH(n)) {				\
				long l;					\
				char *end;				\
									\
				set_errno(0);				\
				l = strtol(v, &end, 0);			\
				if (get_errno() != 0 || (uintptr_t)end -\
				    (uintptr_t)v != vlen) {		\
					malloc_conf_error(		\
					    "Invalid conf value",	\
					    k, klen, v, vlen);		\
				} else if (l < (ssize_t)min || l >	\
				    (ssize_t)max) {			\
					malloc_conf_error(		\
					    "Out-of-range conf value",	\
					    k, klen, v, vlen);		\
				} else					\
					o = l;				\
				continue;				\
			}
#define	CONF_HANDLE_CHAR_P(o, n, d)					\
			if (CONF_MATCH(n)) {				\
				size_t cpylen = (vlen <=		\
				    sizeof(o)-1) ? vlen :		\
				    sizeof(o)-1;			\
				strncpy(o, v, cpylen);			\
				o[cpylen] = '\0';			\
				continue;				\
			}

			CONF_HANDLE_BOOL(opt_abort, "abort", true)
			/*
			 * Chunks always require at least one header page, plus
			 * one data page in the absence of redzones, or three
			 * pages in the presence of redzones.  In order to
			 * simplify options processing, fix the limit based on
			 * config_fill.
			 */
			CONF_HANDLE_SIZE_T(opt_lg_chunk, "lg_chunk", LG_PAGE +
			    (config_fill ? 2 : 1), (sizeof(size_t) << 3) - 1,
			    true)
			if (strncmp("dss", k, klen) == 0) {
				int i;
				bool match = false;
				for (i = 0; i < dss_prec_limit; i++) {
					if (strncmp(dss_prec_names[i], v, vlen)
					    == 0) {
						if (chunk_dss_prec_set(i)) {
							malloc_conf_error(
							    "Error setting dss",
							    k, klen, v, vlen);
						} else {
							opt_dss =
							    dss_prec_names[i];
							match = true;
							break;
						}
					}
				}
				if (match == false) {
					malloc_conf_error("Invalid conf value",
					    k, klen, v, vlen);
				}
				continue;
			}
			CONF_HANDLE_SIZE_T(opt_narenas, "narenas", 1,
			    SIZE_T_MAX, false)
			CONF_HANDLE_SSIZE_T(opt_lg_dirty_mult, "lg_dirty_mult",
			    -1, (sizeof(size_t) << 3) - 1)
			CONF_HANDLE_BOOL(opt_stats_print, "stats_print", true)
			if (config_fill) {
				CONF_HANDLE_BOOL(opt_junk, "junk", true)
				CONF_HANDLE_SIZE_T(opt_quarantine, "quarantine",
				    0, SIZE_T_MAX, false)
				CONF_HANDLE_BOOL(opt_redzone, "redzone", true)
				CONF_HANDLE_BOOL(opt_zero, "zero", true)
			}
			if (config_utrace) {
				CONF_HANDLE_BOOL(opt_utrace, "utrace", true)
			}
			if (config_xmalloc) {
				CONF_HANDLE_BOOL(opt_xmalloc, "xmalloc", true)
			}
			if (config_tcache) {
				CONF_HANDLE_BOOL(opt_tcache, "tcache",
				    !config_valgrind || !in_valgrind)
				if (CONF_MATCH("tcache")) {
					assert(config_valgrind && in_valgrind);
					if (opt_tcache) {
						opt_tcache = false;
						malloc_conf_error(
						"tcache cannot be enabled "
						"while running inside Valgrind",
						k, klen, v, vlen);
					}
					continue;
				}
				CONF_HANDLE_SSIZE_T(opt_lg_tcache_max,
				    "lg_tcache_max", -1,
				    (sizeof(size_t) << 3) - 1)
			}
			if (config_prof) {
				CONF_HANDLE_BOOL(opt_prof, "prof", true)
				CONF_HANDLE_CHAR_P(opt_prof_prefix,
				    "prof_prefix", "jeprof")
				CONF_HANDLE_BOOL(opt_prof_active, "prof_active",
				    true)
				CONF_HANDLE_SSIZE_T(opt_lg_prof_sample,
				    "lg_prof_sample", 0,
				    (sizeof(uint64_t) << 3) - 1)
				CONF_HANDLE_BOOL(opt_prof_accum, "prof_accum",
				    true)
				CONF_HANDLE_SSIZE_T(opt_lg_prof_interval,
				    "lg_prof_interval", -1,
				    (sizeof(uint64_t) << 3) - 1)
				CONF_HANDLE_BOOL(opt_prof_gdump, "prof_gdump",
				    true)
				CONF_HANDLE_BOOL(opt_prof_final, "prof_final",
				    true)
				CONF_HANDLE_BOOL(opt_prof_leak, "prof_leak",
				    true)
			}
			malloc_conf_error("Invalid conf pair", k, klen, v,
			    vlen);
#undef CONF_MATCH
#undef CONF_HANDLE_BOOL
#undef CONF_HANDLE_SIZE_T
#undef CONF_HANDLE_SSIZE_T
#undef CONF_HANDLE_CHAR_P
		}
	}
}

static bool
malloc_init_hard(void)
{
	malloc_mutex_lock(&init_lock);
	if (malloc_initialized || IS_INITIALIZER) {
		/*
		 * Another thread initialized the allocator before this one
		 * acquired init_lock, or this thread is the initializing
		 * thread, and it is recursively allocating.
		 */
		malloc_mutex_unlock(&init_lock);
		return (false);
	}
#ifdef JEMALLOC_THREADED_INIT
	if (malloc_initializer != NO_INITIALIZER && IS_INITIALIZER == false) {
		/* Busy-wait until the initializing thread completes. */
		do {
			malloc_mutex_unlock(&init_lock);
			CPU_SPINWAIT;
			malloc_mutex_lock(&init_lock);
		} while (malloc_initialized == false);
		malloc_mutex_unlock(&init_lock);
		return (false);
	}
#endif
	malloc_initializer = INITIALIZER;

	malloc_tsd_boot();
	if (config_prof)
		prof_boot0();

	malloc_conf_init();

	if (opt_stats_print) {
		/* Print statistics at exit. */
		if (atexit(stats_print_atexit) != 0) {
			malloc_write("<jemalloc>: Error in atexit()\n");
			if (opt_abort)
				abort();
		}
	}

	pools_shared_data_initialized = false;

	je_base_malloc = base_malloc_default;
	je_base_free = base_free_default;

	if (chunk_global_boot()) {
		malloc_mutex_unlock(&init_lock);
		return (true);
	}

	if (ctl_boot()) {
		malloc_mutex_unlock(&init_lock);
		return (true);
	}

	if (config_prof)
		prof_boot1();

	arena_boot();

	pool_boot();

	/* Initialize allocation counters before any allocations can occur. */
	if (config_stats && thread_allocated_tsd_boot()) {
		malloc_mutex_unlock(&init_lock);
		return (true);
	}

	if (arenas_tsd_boot()) {
		malloc_mutex_unlock(&init_lock);
		return (true);
	}

	if (config_tcache && tcache_boot1()) {
		malloc_mutex_unlock(&init_lock);
		return (true);
	}

	if (config_fill && quarantine_boot()) {
		malloc_mutex_unlock(&init_lock);
		return (true);
	}

	if (config_prof && prof_boot2()) {
		malloc_mutex_unlock(&init_lock);
		return (true);
	}

	malloc_mutex_unlock(&init_lock);
	/**********************************************************************/
	/* Recursive allocation may follow. */

	ncpus = malloc_ncpus();

#if (!defined(JEMALLOC_MUTEX_INIT_CB) && !defined(JEMALLOC_ZONE) \
    && !defined(_WIN32) && !defined(__native_client__))
	/* LinuxThreads's pthread_atfork() allocates. */
	if (pthread_atfork(jemalloc_prefork, jemalloc_postfork_parent,
	    jemalloc_postfork_child) != 0) {
		malloc_write("<jemalloc>: Error in pthread_atfork()\n");
		if (opt_abort)
			abort();
	}
#endif

	/* Done recursively allocating. */
	/**********************************************************************/
	malloc_mutex_lock(&init_lock);

	if (mutex_boot()) {
		malloc_mutex_unlock(&init_lock);
		return (true);
	}

	if (opt_narenas == 0) {
		/*
		 * For SMP systems, create more than one arena per CPU by
		 * default.
		 */
		if (ncpus > 1)
			opt_narenas = ncpus << 2;
		else
			opt_narenas = 1;
	}

	malloc_initialized = true;
	malloc_mutex_unlock(&init_lock);

	return (false);
}

/*
 * End initialization functions.
 */
/******************************************************************************/
/*
 * Begin malloc(3)-compatible functions.
 */

static void *
imalloc_prof_sample(size_t usize, prof_thr_cnt_t *cnt)
{
	void *p;

	if (cnt == NULL)
		return (NULL);
	if (usize <= SMALL_MAXCLASS) {
		p = imalloc(SMALL_MAXCLASS+1);
		if (p == NULL)
			return (NULL);
		arena_prof_promoted(p, usize);
	} else
		p = imalloc(usize);

	return (p);
}

JEMALLOC_ALWAYS_INLINE_C void *
imalloc_prof(size_t usize)
{
	void *p;
	prof_thr_cnt_t *cnt;

	PROF_ALLOC_PREP(usize, cnt);
	if ((uintptr_t)cnt != (uintptr_t)1U)
		p = imalloc_prof_sample(usize, cnt);
	else
		p = imalloc(usize);
	if (p == NULL)
		return (NULL);
	prof_malloc(p, usize, cnt);

	return (p);
}

JEMALLOC_ALWAYS_INLINE_C void *
imalloc_body(size_t size, size_t *usize)
{

	if (malloc_init_base_pool())
		return (NULL);

	if (config_prof && opt_prof) {
		*usize = s2u(size);
		return (imalloc_prof(*usize));
	}

	if (config_stats || (config_valgrind && in_valgrind))
		*usize = s2u(size);
	return (imalloc(size));
}

void *
je_malloc(size_t size)
{
	void *ret;
	size_t usize JEMALLOC_CC_SILENCE_INIT(0);

	if (size == 0)
		size = 1;

	ret = imalloc_body(size, &usize);
	if (ret == NULL) {
		if (config_xmalloc && opt_xmalloc) {
			malloc_write("<jemalloc>: Error in malloc(): "
			    "out of memory\n");
			abort();
		}
		set_errno(ENOMEM);
	}
	if (config_stats && ret != NULL) {
		assert(usize == isalloc(ret, config_prof));
		thread_allocated_tsd_get()->allocated += usize;
	}
	UTRACE(0, size, ret);
	JEMALLOC_VALGRIND_MALLOC(ret != NULL, ret, usize, false);
	return (ret);
}

static void *
imemalign_prof_sample(size_t alignment, size_t usize, prof_thr_cnt_t *cnt)
{
	void *p;

	if (cnt == NULL)
		return (NULL);
	if (usize <= SMALL_MAXCLASS) {
		assert(sa2u(SMALL_MAXCLASS+1, alignment) != 0);
		p = ipalloc(sa2u(SMALL_MAXCLASS+1, alignment), alignment,
		    false);
		if (p == NULL)
			return (NULL);
		arena_prof_promoted(p, usize);
	} else
		p = ipalloc(usize, alignment, false);

	return (p);
}

JEMALLOC_ALWAYS_INLINE_C void *
imemalign_prof(size_t alignment, size_t usize, prof_thr_cnt_t *cnt)
{
	void *p;

	if ((uintptr_t)cnt != (uintptr_t)1U)
		p = imemalign_prof_sample(alignment, usize, cnt);
	else
		p = ipalloc(usize, alignment, false);
	if (p == NULL)
		return (NULL);
	prof_malloc(p, usize, cnt);

	return (p);
}

JEMALLOC_ATTR(nonnull(1))
static int
imemalign(void **memptr, size_t alignment, size_t size, size_t min_alignment)
{
	int ret;
	size_t usize;
	void *result;

	assert(min_alignment != 0);

	if (malloc_init_base_pool()) {
		result = NULL;
		goto label_oom;
	} else {
		if (size == 0)
			size = 1;

		/* Make sure that alignment is a large enough power of 2. */
		if (((alignment - 1) & alignment) != 0
		    || (alignment < min_alignment)) {
			if (config_xmalloc && opt_xmalloc) {
				malloc_write("<jemalloc>: Error allocating "
				    "aligned memory: invalid alignment\n");
				abort();
			}
			result = NULL;
			ret = EINVAL;
			goto label_return;
		}

		usize = sa2u(size, alignment);
		if (usize == 0) {
			result = NULL;
			goto label_oom;
		}

		if (config_prof && opt_prof) {
			prof_thr_cnt_t *cnt;

			PROF_ALLOC_PREP(usize, cnt);
			result = imemalign_prof(alignment, usize, cnt);
		} else
			result = ipalloc(usize, alignment, false);
		if (result == NULL)
			goto label_oom;
	}

	*memptr = result;
	ret = 0;
label_return:
	if (config_stats && result != NULL) {
		assert(usize == isalloc(result, config_prof));
		thread_allocated_tsd_get()->allocated += usize;
	}
	UTRACE(0, size, result);
	return (ret);
label_oom:
	assert(result == NULL);
	if (config_xmalloc && opt_xmalloc) {
		malloc_write("<jemalloc>: Error allocating aligned memory: "
		    "out of memory\n");
		abort();
	}
	ret = ENOMEM;
	goto label_return;
}

int
je_posix_memalign(void **memptr, size_t alignment, size_t size)
{
	int ret = imemalign(memptr, alignment, size, sizeof(void *));
	JEMALLOC_VALGRIND_MALLOC(ret == 0, *memptr, isalloc(*memptr,
	    config_prof), false);
	return (ret);
}

void *
je_aligned_alloc(size_t alignment, size_t size)
{
	void *ret;
	int err;

	if ((err = imemalign(&ret, alignment, size, 1)) != 0) {
		ret = NULL;
		set_errno(err);
	}
	JEMALLOC_VALGRIND_MALLOC(err == 0, ret, isalloc(ret, config_prof),
	    false);
	return (ret);
}

static void *
icalloc_prof_sample(size_t usize, prof_thr_cnt_t *cnt)
{
	void *p;

	if (cnt == NULL)
		return (NULL);
	if (usize <= SMALL_MAXCLASS) {
		p = icalloc(SMALL_MAXCLASS+1);
		if (p == NULL)
			return (NULL);
		arena_prof_promoted(p, usize);
	} else
		p = icalloc(usize);

	return (p);
}

JEMALLOC_ALWAYS_INLINE_C void *
icalloc_prof(size_t usize, prof_thr_cnt_t *cnt)
{
	void *p;

	if ((uintptr_t)cnt != (uintptr_t)1U)
		p = icalloc_prof_sample(usize, cnt);
	else
		p = icalloc(usize);
	if (p == NULL)
		return (NULL);
	prof_malloc(p, usize, cnt);

	return (p);
}

void *
je_calloc(size_t num, size_t size)
{
	void *ret;
	size_t num_size;
	size_t usize JEMALLOC_CC_SILENCE_INIT(0);

	if (malloc_init_base_pool()) {
		num_size = 0;
		ret = NULL;
		goto label_return;
	}

	num_size = num * size;
	if (num_size == 0) {
		if (num == 0 || size == 0)
			num_size = 1;
		else {
			ret = NULL;
			goto label_return;
		}
	/*
	 * Try to avoid division here.  We know that it isn't possible to
	 * overflow during multiplication if neither operand uses any of the
	 * most significant half of the bits in a size_t.
	 */
	} else if (((num | size) & (SIZE_T_MAX << (sizeof(size_t) << 2)))
	    && (num_size / size != num)) {
		/* size_t overflow. */
		ret = NULL;
		goto label_return;
	}

	if (config_prof && opt_prof) {
		prof_thr_cnt_t *cnt;

		usize = s2u(num_size);
		PROF_ALLOC_PREP(usize, cnt);
		ret = icalloc_prof(usize, cnt);
	} else {
		if (config_stats || (config_valgrind && in_valgrind))
			usize = s2u(num_size);
		ret = icalloc(num_size);
	}

label_return:
	if (ret == NULL) {
		if (config_xmalloc && opt_xmalloc) {
			malloc_write("<jemalloc>: Error in calloc(): out of "
			    "memory\n");
			abort();
		}
		set_errno(ENOMEM);
	}
	if (config_stats && ret != NULL) {
		assert(usize == isalloc(ret, config_prof));
		thread_allocated_tsd_get()->allocated += usize;
	}
	UTRACE(0, num_size, ret);
	JEMALLOC_VALGRIND_MALLOC(ret != NULL, ret, usize, true);
	return (ret);
}

static void *
irealloc_prof_sample(void *oldptr, size_t usize, prof_thr_cnt_t *cnt)
{
	void *p;

	if (cnt == NULL)
		return (NULL);
	if (usize <= SMALL_MAXCLASS) {
		p = iralloc(oldptr, SMALL_MAXCLASS+1, 0, 0, false);
		if (p == NULL)
			return (NULL);
		arena_prof_promoted(p, usize);
	} else
		p = iralloc(oldptr, usize, 0, 0, false);

	return (p);
}

JEMALLOC_ALWAYS_INLINE_C void *
irealloc_prof(void *oldptr, size_t old_usize, size_t usize, prof_thr_cnt_t *cnt)
{
	void *p;
	prof_ctx_t *old_ctx;

	old_ctx = prof_ctx_get(oldptr);
	if ((uintptr_t)cnt != (uintptr_t)1U)
		p = irealloc_prof_sample(oldptr, usize, cnt);
	else
		p = iralloc(oldptr, usize, 0, 0, false);
	if (p == NULL)
		return (NULL);
	prof_realloc(p, usize, cnt, old_usize, old_ctx);

	return (p);
}

JEMALLOC_INLINE_C void
ifree(void *ptr)
{
	size_t usize;
	UNUSED size_t rzsize JEMALLOC_CC_SILENCE_INIT(0);

	assert(ptr != NULL);
	assert(malloc_initialized || IS_INITIALIZER);

	if (config_prof && opt_prof) {
		usize = isalloc(ptr, config_prof);
		prof_free(ptr, usize);
	} else if (config_stats || config_valgrind)
		usize = isalloc(ptr, config_prof);
	if (config_stats)
		thread_allocated_tsd_get()->deallocated += usize;
	if (config_valgrind && in_valgrind)
		rzsize = p2rz(ptr);
	iqalloc(ptr);
	JEMALLOC_VALGRIND_FREE(ptr, rzsize);
}

void *
je_realloc(void *ptr, size_t size)
{
	void *ret;
	size_t usize JEMALLOC_CC_SILENCE_INIT(0);
	size_t old_usize = 0;
	UNUSED size_t old_rzsize JEMALLOC_CC_SILENCE_INIT(0);

	if (size == 0) {
		if (ptr != NULL) {
			/* realloc(ptr, 0) is equivalent to free(ptr). */
			UTRACE(ptr, 0, 0);
			ifree(ptr);
			return (NULL);
		}
		size = 1;
	}

	if (ptr != NULL) {
		assert(malloc_initialized || IS_INITIALIZER);
		malloc_thread_init();

		if ((config_prof && opt_prof) || config_stats ||
		    (config_valgrind && in_valgrind))
			old_usize = isalloc(ptr, config_prof);
		if (config_valgrind && in_valgrind)
			old_rzsize = config_prof ? p2rz(ptr) : u2rz(old_usize);

		if (config_prof && opt_prof) {
			prof_thr_cnt_t *cnt;

			usize = s2u(size);
			PROF_ALLOC_PREP(usize, cnt);
			ret = irealloc_prof(ptr, old_usize, usize, cnt);
		} else {
			if (config_stats || (config_valgrind && in_valgrind))
				usize = s2u(size);
			ret = iralloc(ptr, size, 0, 0, false);
		}
	} else {
		/* realloc(NULL, size) is equivalent to malloc(size). */
		ret = imalloc_body(size, &usize);
	}

	if (ret == NULL) {
		if (config_xmalloc && opt_xmalloc) {
			malloc_write("<jemalloc>: Error in realloc(): "
			    "out of memory\n");
			abort();
		}
		set_errno(ENOMEM);
	}
	if (config_stats && ret != NULL) {
		thread_allocated_t *ta;
		assert(usize == isalloc(ret, config_prof));
		ta = thread_allocated_tsd_get();
		ta->allocated += usize;
		ta->deallocated += old_usize;
	}
	UTRACE(ptr, size, ret);
	JEMALLOC_VALGRIND_REALLOC(true, ret, usize, true, ptr, old_usize,
	    old_rzsize, true, false);
	return (ret);
}

void
je_free(void *ptr)
{

	UTRACE(ptr, 0, 0);
	if (ptr != NULL)
		ifree(ptr);
}

/*
 * End malloc(3)-compatible functions.
 */
/******************************************************************************/
/*
 * Begin non-standard override functions.
 */

#ifdef JEMALLOC_OVERRIDE_MEMALIGN
void *
je_memalign(size_t alignment, size_t size)
{
	void *ret JEMALLOC_CC_SILENCE_INIT(NULL);
	imemalign(&ret, alignment, size, 1);
	JEMALLOC_VALGRIND_MALLOC(ret != NULL, ret, size, false);
	return (ret);
}
#endif

#ifdef JEMALLOC_OVERRIDE_VALLOC
void *
je_valloc(size_t size)
{
	void *ret JEMALLOC_CC_SILENCE_INIT(NULL);
	imemalign(&ret, PAGE, size, 1);
	JEMALLOC_VALGRIND_MALLOC(ret != NULL, ret, size, false);
	return (ret);
}
#endif

/*
 * is_malloc(je_malloc) is some macro magic to detect if jemalloc_defs.h has
 * #define je_malloc malloc
 */
#define	malloc_is_malloc 1
#define	is_malloc_(a) malloc_is_ ## a
#define	is_malloc(a) is_malloc_(a)

#if ((is_malloc(je_malloc) == 1) && defined(__GLIBC__) && !defined(__UCLIBC__))
/*
 * glibc provides the RTLD_DEEPBIND flag for dlopen which can make it possible
 * to inconsistently reference libc's malloc(3)-compatible functions
 * (https://bugzilla.mozilla.org/show_bug.cgi?id=493541).
 *
 * These definitions interpose hooks in glibc.  The functions are actually
 * passed an extra argument for the caller return address, which will be
 * ignored.
 */
JEMALLOC_EXPORT void (*__free_hook)(void *ptr) = je_free;
JEMALLOC_EXPORT void *(*__malloc_hook)(size_t size) = je_malloc;
JEMALLOC_EXPORT void *(*__realloc_hook)(void *ptr, size_t size) = je_realloc;
JEMALLOC_EXPORT void *(*__memalign_hook)(size_t alignment, size_t size) =
    je_memalign;
#endif

/*
 * End non-standard override functions.
 */
/******************************************************************************/
/*
 * Begin non-standard functions.
 */

static void*
base_malloc_default(size_t size)
{

	return base_alloc(&base_pool, size);
}

static void
base_free_default(void *ptr)
{

}

bool
pools_shared_data_create(void)
{
	if (malloc_init())
		return (true);

	if (pools_shared_data_initialized)
		return (false);

	if (config_tcache && tcache_boot0())
		return (true);

	pools_shared_data_initialized = true;

	return (false);
}

void pools_shared_data_destroy(void)
{
	/* Only destroy when no pools exist */
	if (npools == 0) {
		pools_shared_data_initialized = false;

		je_base_free(tcache_bin_info);
		tcache_bin_info = NULL;
	}
}

pool_t *
je_pool_create(void *addr, size_t size, int zeroed)
{
	if (malloc_init())
		return (NULL);

	if (addr == NULL || size < POOL_MINIMAL_SIZE)
		return (NULL);

	pool_t *pool = (pool_t *)addr;
	unsigned pool_id;
	size_t result;

	/* Preinit base pool if not exist, before lock pool_lock */
	if (malloc_init_base_pool())
		return (NULL);

	assert(pools != NULL);
	assert(npools > 0);

	malloc_mutex_lock(&pools_lock);

	/*
	 * Find unused pool ID.
	 * Pool 0 is a special pool with reserved ID. Pool is created during
	 * malloc_init_pool_base() and allocates memory from RAM.
	 */
	for (pool_id = 1; pool_id < npools; ++pool_id) {
		if (pools[pool_id] == NULL)
			break;
	}

	if (pool_id == npools && npools < POOLS_MAX) {
		size_t npools_new = npools * 2;
		pool_t **pools_new = base_alloc(&base_pool,
					npools_new * sizeof (pool_t *));
		if (pools_new == NULL) {
			malloc_mutex_unlock(&pools_lock);
			return (NULL);
		}

		memcpy(pools_new, pools, npools * sizeof (pool_t *));
		memset(&pools_new[npools], 0,
				(npools_new - npools) * sizeof (pool_t *));

		pools = pools_new;
		npools = npools_new;
	}

	if (pool_id == POOLS_MAX) {
		malloc_printf("<jemalloc>: Error in pool_create(): "
			"exceeded max number of pools (%u)\n", POOLS_MAX);
		malloc_mutex_unlock(&pools_lock);
		return (NULL);
	}

	if (!zeroed)
		memset(addr, 0, sizeof (pool_t));

	/* preinit base allocator in unused space, align the address to the cache line */
	pool->base_next_addr = (void *)CACHELINE_CEILING((uintptr_t)addr +
		sizeof (pool_t));
	pool->base_past_addr = (void *)((uintptr_t)addr + size);

	/* prepare pool and internal structures */
	if (pool_new(pool, pool_id)) {
		assert(pools[pool_id] == NULL);
		malloc_mutex_unlock(&pools_lock);
		pools_shared_data_destroy();
		return (NULL);
	}

	/* preallocate the chunk tree nodes for the maximum possible number of chunks */
	result = base_node_prealloc(pool, size/chunksize);
	assert(result == 0);

	assert(pools[pool_id] == NULL);
	pools[pool_id] = pool;
	pools[pool_id]->seqno = ++pool_seqno;
	npools_cnt++;

	malloc_mutex_unlock(&pools_lock);

	pool->memory_range_list = base_alloc(pool, sizeof (*pool->memory_range_list));

	/* pointer to the address of chunks, align the address to chunksize */
	void *usable_addr = (void*)CHUNK_CEILING((uintptr_t)pool->base_next_addr);

	/* reduce end of base allocator up to chunks start */
	pool->base_past_addr = usable_addr;

	/* usable chunks space, must be multiple of chunksize */
	size_t usable_size = (size - (uintptr_t)(usable_addr - addr))
		& ~chunksize_mask;

	assert(usable_size > 0);

	malloc_mutex_lock(&pool->memory_range_mtx);
	pool->memory_range_list->next = NULL;
	pool->memory_range_list->addr = (uintptr_t)addr;
	pool->memory_range_list->addr_end = (uintptr_t)addr + size;
	pool->memory_range_list->usable_addr = (uintptr_t)usable_addr;
	pool->memory_range_list->usable_addr_end = (uintptr_t)usable_addr + usable_size;
	malloc_mutex_unlock(&pool->memory_range_mtx);

	/* register the usable pool space as a single big chunk */
	chunk_record(pool,
		&pool->chunks_szad_mmap, &pool->chunks_ad_mmap,
		usable_addr, usable_size, zeroed);

	pool->ctl_initialized = false;

	return (pool);
}

int
je_pool_delete(pool_t *pool)
{
	unsigned pool_id = pool->pool_id;

	/* Remove pool from global array */
	malloc_mutex_lock(&pools_lock);

	if ((pool_id == 0) || (pool_id >= npools) || (pools[pool_id] != pool)) {
		malloc_mutex_unlock(&pools_lock);
		malloc_printf("<jemalloc>: Error in pool_delete(): "
			"invalid pool_id (%u)\n", pool_id);
		return -1;
	}

	pool_destroy(pool);
	pools[pool_id] = NULL;
	npools_cnt--;

	/*
	 * TODO: Destroy mutex
	 * base_mtx
	 */

	pools_shared_data_destroy();

	malloc_mutex_unlock(&pools_lock);
	return 0;
}

static int
check_is_unzeroed(void *ptr, size_t size)
{
	size_t i;
	size_t *p = (size_t *)ptr;
	size /= sizeof(size_t);
	for (i = 0; i < size; i++) {
		if (p[i])
			return 1;
	}
	return 0;
}

static extent_node_t *
check_tree_binary_iter_cb(extent_tree_t *tree, extent_node_t *node, void *arg)
{
	check_data_cb_t *arg_cb = arg;

	if (node->size == 0) {
		arg_cb->error += 1;
		malloc_printf("<jemalloc>: Error in pool_check(): "
			"chunk 0x%p size is zero\n", node);
		/* returns value other than NULL to break iteration */
		return (void*)(UINTPTR_MAX);
	}

	arg_cb->size += node->size;

	if (node->zeroed && check_is_unzeroed(node->addr, node->size)) {
		arg_cb->error += 1;
		malloc_printf("<jemalloc>: Error in pool_check(): "
			"chunk 0x%p, is marked as zeroed, but is dirty\n",
			node->addr);
		/* returns value other than NULL to break iteration */
		return (void*)(UINTPTR_MAX);
	}

	/* check chunks address is inside pool memory */
	pool_memory_range_node_t *list = arg_cb->list;
	uintptr_t addr = (uintptr_t)node->addr;
	uintptr_t addr_end = (uintptr_t)node->addr + node->size;
	while (list != NULL) {
		if ((list->usable_addr <= addr) &&
			(addr < list->usable_addr_end) &&
			(list->usable_addr < addr_end) &&
			(addr_end <= list->usable_addr_end)) {
				/* return NULL to continue iterations of tree */
				return (NULL);
		}
		list = list->next;
	}

	arg_cb->error += 1;
	malloc_printf("<jemalloc>: Error in pool_check(): "
			"incorrect address chunk 0x%p, out of memory pool\n",
			node->addr);

	/* returns value other than NULL to break iteration */
	return (void*)(UINTPTR_MAX);
}

static arena_chunk_map_t *
check_tree_chunks_avail_iter_cb(arena_avail_tree_t *tree,
	arena_chunk_map_t *map, void *arg)
{
	check_data_cb_t *arg_cb = arg;

	if ((map->bits & (CHUNK_MAP_LARGE|CHUNK_MAP_ALLOCATED)) != 0) {
		arg_cb->error += 1;
		malloc_printf("<jemalloc>: Error in pool_check(): "
			"flags in map->bits %zu are incorrect\n", map->bits);
		/* returns value other than NULL to break iteration */
		return (void*)(UINTPTR_MAX);
	}

	if ((map->bits & ~PAGE_MASK) == 0) {
		arg_cb->error += 1;
		malloc_printf("<jemalloc>: Error in pool_check(): "
			"chunk_map 0x%p size is zero\n", map);
		/* returns value other than NULL to break iteration */
		return (void*)(UINTPTR_MAX);
	}

	size_t chunk_size = (map->bits & ~PAGE_MASK);
	arg_cb->size += chunk_size;

	arena_chunk_t *run_chunk = CHUNK_ADDR2BASE(map);
	size_t pageind = arena_mapelm_to_pageind(map);
	void *chunk_addr = (void *)((uintptr_t)run_chunk + (pageind << LG_PAGE));

	if (((map->bits & (CHUNK_MAP_UNZEROED | CHUNK_MAP_DIRTY)) == 0) &&
			check_is_unzeroed(chunk_addr, chunk_size)) {
		arg_cb->error += 1;
		malloc_printf("<jemalloc>: Error in pool_check(): "
			"chunk_map 0x%p, is marked as zeroed, but is dirty\n",
			map);
		/* returns value other than NULL to break iteration */
		return (void*)(UINTPTR_MAX);
	}

	/* check chunks address is inside pool memory */
	pool_memory_range_node_t *list = arg_cb->list;
	uintptr_t addr = (uintptr_t)chunk_addr;
	uintptr_t addr_end = (uintptr_t)chunk_addr +  chunk_size;
	while (list != NULL) {
		if ((list->usable_addr <= addr) &&
			(addr < list->usable_addr_end) &&
			(list->usable_addr < addr_end) &&
			(addr_end <= list->usable_addr_end)) {
				/* return NULL to continue iterations of tree */
				return (NULL);
		}
		list = list->next;
	}

	arg_cb->error += 1;
	malloc_printf("<jemalloc>: Error in pool_check(): "
			"incorrect address chunk_map 0x%p, out of memory pool\n",
			chunk_addr);

	/* returns value other than NULL to break iteration */
	return (void*)(UINTPTR_MAX);
}

int
je_pool_check(pool_t *pool)
{
	size_t total_size = 0;
	unsigned i;
	pool_memory_range_node_t *node;

	malloc_mutex_lock(&pools_lock);
	if ((pool->pool_id == 0) || (pool->pool_id >= npools)) {
		malloc_write("<jemalloc>: Error in pool_check(): "
				"invalid pool id\n");
		malloc_mutex_unlock(&pools_lock);
		return -1;
	}

	if (pools[pool->pool_id] != pool) {
		malloc_write("<jemalloc>: Error in pool_check(): "
				"invalid pool handle, probably pool was deleted\n");
		malloc_mutex_unlock(&pools_lock);
		return -1;
	}
	malloc_mutex_unlock(&pools_lock);

	malloc_mutex_lock(&pool->memory_range_mtx);

	/* check memory regions defined correctly */
	node = pool->memory_range_list;
	while (node != NULL) {
		total_size += node->usable_addr_end - node->usable_addr;
		if ((node->addr > node->usable_addr) ||
			(node->addr_end < node->usable_addr_end) ||
			(node->usable_addr >= node->usable_addr_end)) {
			malloc_write("<jemalloc>: Error in pool_check(): "
					"corrupted pool memory\n");
			malloc_mutex_unlock(&pool->memory_range_mtx);
			return 0;
		}
		node = node->next;
	}

	/* check memory collision with other pools */
	malloc_mutex_lock(&pools_lock);
	for (i = 1; i < npools; i++) {
		pool_t *pool_cmp = pools[i];
		if (pool_cmp != NULL && i != pool->pool_id) {
			node = pool->memory_range_list;
			while (node != NULL) {
				pool_memory_range_node_t *node2 = pool_cmp->memory_range_list;
				while (node2 != NULL) {
					if ((node->addr <= node2->addr &&
							node2->addr < node->addr_end) ||
							(node2->addr <= node->addr &&
							node->addr < node2->addr_end)) {

						malloc_mutex_unlock(&pools_lock);
						malloc_write("<jemalloc>: Error in pool_check(): "
							"pool uses the same as another pool\n");
						malloc_mutex_unlock(&pool->memory_range_mtx);
						return 0;
					}
					node2 = node2->next;
				}
				node = node->next;
			}
		}
	}
	malloc_mutex_unlock(&pools_lock);

	/* check the addresses of the chunks are inside memory region */
	check_data_cb_t arg_cb;
	arg_cb.list = pool->memory_range_list;
	arg_cb.size = 0;
	arg_cb.error = 0;

	malloc_mutex_lock(&pool->chunks_mtx);
	malloc_rwlock_wrlock(&pool->arenas_lock);
	extent_tree_szad_iter(&pool->chunks_szad_mmap, NULL,
		check_tree_binary_iter_cb, &arg_cb);

	for (i = 0; i < pool->narenas_total && arg_cb.error == 0; ++i) {
		arena_t *arena = pool->arenas[i];
		if (arena != NULL) {
			malloc_mutex_lock(&arena->lock);

			arena_runs_avail_tree_iter(arena, check_tree_chunks_avail_iter_cb,
				&arg_cb);

			arena_chunk_t *spare = arena->spare;
			if (spare != NULL) {
				size_t spare_size = arena_mapbits_unallocated_size_get(spare,
					map_bias);

				arg_cb.size += spare_size;

				/* check that spare is zeroed */
				if ((arena_mapbits_unzeroed_get(spare, map_bias) == 0) &&
						check_is_unzeroed(
							(void *)((uintptr_t)spare + (map_bias << LG_PAGE)),
							spare_size)) {

					arg_cb.error += 1;
					malloc_printf("<jemalloc>: Error in pool_check(): "
						"spare 0x%p, is marked as zeroed, but is dirty\n",
						spare);
				}
			}
			malloc_mutex_unlock(&arena->lock);
		}
	}

	malloc_rwlock_unlock(&pool->arenas_lock);
	malloc_mutex_unlock(&pool->chunks_mtx);

	malloc_mutex_unlock(&pool->memory_range_mtx);

	if (arg_cb.error != 0) {
		return 0;
	}

	if (total_size < arg_cb.size) {
		malloc_printf("<jemalloc>: Error in pool_check(): total size of all "
			"chunks: %zu is greater than associated memory range size: %zu\n",
			arg_cb.size, total_size);
		return 0;
	}

	return 1;
}

/*
 * add more memory to a pool
 */
size_t
je_pool_extend(pool_t *pool, void *addr, size_t size, int zeroed)
{
	void *usable_addr = addr;
	size_t nodes_number = size/chunksize;
	if (size < POOL_MINIMAL_SIZE)
		return 0;

	/* preallocate the chunk tree nodes for the max possible number of chunks */
	nodes_number = base_node_prealloc(pool, nodes_number);
	pool_memory_range_node_t *node = base_alloc(pool,
		sizeof (*pool->memory_range_list));

	if (nodes_number > 0 || node == NULL) {
		/*
		 * If base allocation using existing chunks fails, then use the new
		 * chunk as a source for further base allocations.
		 */
		malloc_mutex_lock(&pool->base_mtx);
		/* preinit base allocator in unused space */
		pool->base_next_addr = (void *)CACHELINE_CEILING((uintptr_t)addr);
		pool->base_past_addr = (void *)((uintptr_t)addr + size);
		malloc_mutex_unlock(&pool->base_mtx);

		if (nodes_number > 0)
			nodes_number = base_node_prealloc(pool, nodes_number);
		assert(nodes_number == 0);

		if (node == NULL)
			node = base_alloc(pool, sizeof (*pool->memory_range_list));
		assert(node != NULL);

		/* pointer to the address of chunks, align the address to chunksize */
		usable_addr = (void*)CHUNK_CEILING((uintptr_t)pool->base_next_addr);
		/* reduce end of base allocator up to chunks */
		pool->base_past_addr = usable_addr;
	}

	usable_addr = (void *)CHUNK_CEILING((uintptr_t)usable_addr);

	size_t usable_size = (size - (uintptr_t)(usable_addr - addr))
		& ~chunksize_mask;

	assert(usable_size > 0);

	node->addr = (uintptr_t)addr;
	node->addr_end = (uintptr_t)addr + size;
	node->usable_addr = (uintptr_t)usable_addr;
	node->usable_addr_end = (uintptr_t)usable_addr + usable_size;

	malloc_mutex_lock(&pool->memory_range_mtx);
	node->next = pool->memory_range_list;
	pool->memory_range_list = node;

	chunk_record(pool,
		&pool->chunks_szad_mmap, &pool->chunks_ad_mmap,
		usable_addr, usable_size, zeroed);

	malloc_mutex_unlock(&pool->memory_range_mtx);

	return usable_size;
}

static void *
pool_ialloc_prof_sample(pool_t *pool, size_t usize, prof_thr_cnt_t *cnt,
	void *(*ialloc)(pool_t *, size_t))
{
	void *p;

	if (cnt == NULL)
		return (NULL);
	if (usize <= SMALL_MAXCLASS) {
		p = ialloc(pool, SMALL_MAXCLASS+1);
		if (p == NULL)
			return (NULL);
		arena_prof_promoted(p, usize);
	} else
		p = ialloc(pool, usize);

	return (p);
}

JEMALLOC_ALWAYS_INLINE_C void *
pool_ialloc_prof(pool_t *pool, size_t usize,
	void *(*ialloc)(pool_t *, size_t))
{
	void *p;
	prof_thr_cnt_t *cnt;

	PROF_ALLOC_PREP(usize, cnt);
	if ((uintptr_t)cnt != (uintptr_t)1U)
		p = pool_ialloc_prof_sample(pool, usize, cnt, ialloc);
	else
		p = ialloc(pool, usize);
	if (p == NULL)
		return (NULL);
	prof_malloc(p, usize, cnt);

	return (p);
}

JEMALLOC_ALWAYS_INLINE_C void *
pool_imalloc_body(pool_t *pool, size_t size, size_t *usize)
{

	if (malloc_init())
		return (NULL);

	if (config_prof && opt_prof) {
		*usize = s2u(size);
		return (pool_ialloc_prof(pool, *usize, pool_imalloc));
	}

	if (config_stats || (config_valgrind && in_valgrind))
		*usize = s2u(size);
	return (pool_imalloc(pool, size));
}

void *
je_pool_malloc(pool_t *pool, size_t size)
{
	void *ret;
	size_t usize JEMALLOC_CC_SILENCE_INIT(0);

	if (size == 0)
		size = 1;

	ret = pool_imalloc_body(pool, size, &usize);
	if (ret == NULL) {
		if (config_xmalloc && opt_xmalloc) {
			malloc_write("<jemalloc>: Error in pool_malloc(): "
			    "out of memory\n");
			abort();
		}
		set_errno(ENOMEM);
	}
	if (config_stats && ret != NULL) {
		assert(usize == isalloc(ret, config_prof));
		thread_allocated_tsd_get()->allocated += usize;
	}
	UTRACE(0, size, ret);
	JEMALLOC_VALGRIND_MALLOC(ret != NULL, ret, usize, false);
	return (ret);
}

void *
je_pool_calloc(pool_t *pool, size_t num, size_t size)
{
	void *ret;
	size_t usize JEMALLOC_CC_SILENCE_INIT(0);
	size_t num_size;

	num_size = num * size;
	if (num_size == 0) {
		if (num == 0 || size == 0)
			num_size = 1;
		else {
			ret = NULL;
			goto label_return;
		}

	} else if (((num | size) & (SIZE_T_MAX << (sizeof(size_t) << 2)))
	    && (num_size / size != num)) {
		ret = NULL;
		goto label_return;
	}

	if (config_prof && opt_prof) {
		usize = s2u(num_size);
		ret = pool_ialloc_prof(pool, usize, pool_icalloc);
	} else {
		if (config_stats || (config_valgrind && in_valgrind))
			usize = s2u(num_size);
		ret = pool_icalloc(pool, num_size);
	}

label_return:
	if (ret == NULL) {
		if (config_xmalloc && opt_xmalloc) {
			malloc_write("<jemalloc>: Error in pool_calloc(): "
				"out of memory\n");
			abort();
		}
		set_errno(ENOMEM);
	}
	if (config_stats && ret != NULL) {
		assert(usize == isalloc(ret, config_prof));
		thread_allocated_tsd_get()->allocated += usize;
	}
	UTRACE(0, num_size, ret);
	JEMALLOC_VALGRIND_MALLOC(ret != NULL, ret, usize, true);
	return (ret);
}

static void *
pool_irealloc_prof_sample(pool_t *pool, void *oldptr, size_t usize,
	prof_thr_cnt_t *cnt)
{
	void *p;

	if (cnt == NULL)
		return (NULL);
	if (usize <= SMALL_MAXCLASS) {
		p = pool_iralloc(pool, oldptr, SMALL_MAXCLASS+1, 0, 0, false);
		if (p == NULL)
			return (NULL);
		arena_prof_promoted(p, usize);
	} else
		p = pool_iralloc(pool, oldptr, usize, 0, 0, false);

	return (p);
}

JEMALLOC_ALWAYS_INLINE_C void *
pool_irealloc_prof(pool_t *pool, void *oldptr, size_t old_usize,
	size_t usize, prof_thr_cnt_t *cnt)
{
	void *p;
	prof_ctx_t *old_ctx;

	old_ctx = prof_ctx_get(oldptr);
	if ((uintptr_t)cnt != (uintptr_t)1U)
		p = pool_irealloc_prof_sample(pool, oldptr, usize, cnt);
	else
		p = pool_iralloc(pool, oldptr, usize, 0, 0, false);
	if (p == NULL)
		return (NULL);
	prof_realloc(p, usize, cnt, old_usize, old_ctx);

	return (p);
}

JEMALLOC_INLINE_C void
pool_ifree(pool_t *pool, void *ptr)
{
	size_t usize;
	UNUSED size_t rzsize JEMALLOC_CC_SILENCE_INIT(0);
	arena_chunk_t *chunk;

	assert(ptr != NULL);
	assert(malloc_initialized || IS_INITIALIZER);

	if (config_prof && opt_prof) {
		usize = isalloc(ptr, config_prof);
		prof_free(ptr, usize);
	} else if (config_stats || config_valgrind)
		usize = isalloc(ptr, config_prof);
	if (config_stats)
		thread_allocated_tsd_get()->deallocated += usize;
	if (config_valgrind && in_valgrind)
		rzsize = p2rz(ptr);

	chunk = (arena_chunk_t *)CHUNK_ADDR2BASE(ptr);
	if (chunk != ptr)
		arena_dalloc(chunk, ptr, true);
	else
		huge_dalloc(pool, ptr);

	JEMALLOC_VALGRIND_FREE(ptr, rzsize);
}

void *
je_pool_ralloc(pool_t *pool, void *ptr, size_t size)
{
	void *ret;
	size_t usize JEMALLOC_CC_SILENCE_INIT(0);
	size_t old_usize = 0;
	UNUSED size_t old_rzsize JEMALLOC_CC_SILENCE_INIT(0);

	if (size == 0) {
		if (ptr != NULL) {
			/* realloc(ptr, 0) is equivalent to free(ptr). */
			UTRACE(ptr, 0, 0);
			pool_ifree(pool, ptr);
			return (NULL);
		}
		size = 1;
	}

	if (ptr != NULL) {
		assert(malloc_initialized || IS_INITIALIZER);
		malloc_init();

		if ((config_prof && opt_prof) || config_stats ||
		    (config_valgrind && in_valgrind))
			old_usize = isalloc(ptr, config_prof);
		if (config_valgrind && in_valgrind)
			old_rzsize = config_prof ? p2rz(ptr) : u2rz(old_usize);

		if (config_prof && opt_prof) {
			prof_thr_cnt_t *cnt;

			usize = s2u(size);
			PROF_ALLOC_PREP(usize, cnt);
			ret = pool_irealloc_prof(pool, ptr, old_usize,
				usize, cnt);
		} else {
			if (config_stats || (config_valgrind && in_valgrind))
				usize = s2u(size);
			ret = pool_iralloc(pool, ptr, size, 0, 0, false);
		}
	} else {
		/* realloc(NULL, size) is equivalent to malloc(size). */
		ret = pool_imalloc_body(pool, size, &usize);
	}

	if (ret == NULL) {
		if (config_xmalloc && opt_xmalloc) {
			malloc_write("<jemalloc>: Error in pool_ralloc(): "
			    "out of memory\n");
			abort();
		}
		set_errno(ENOMEM);
	}
	if (config_stats && ret != NULL) {
		thread_allocated_t *ta;
		assert(usize == isalloc(ret, config_prof));
		ta = thread_allocated_tsd_get();
		ta->allocated += usize;
		ta->deallocated += old_usize;
	}
	UTRACE(ptr, size, ret);
	JEMALLOC_VALGRIND_REALLOC(true, ret, usize, true, ptr, old_usize,
	    old_rzsize, true, false);
	return (ret);
}

static void *
pool_imemalign_prof_sample(pool_t *pool, size_t alignment, size_t usize,
	prof_thr_cnt_t *cnt)
{
	void *p;

	if (cnt == NULL)
		return (NULL);
	if (usize <= SMALL_MAXCLASS) {
		assert(sa2u(SMALL_MAXCLASS+1, alignment) != 0);
		p = pool_ipalloc(pool, sa2u(SMALL_MAXCLASS+1, alignment),
			alignment, false);
		if (p == NULL)
			return (NULL);
		arena_prof_promoted(p, usize);
	} else
		p = pool_ipalloc(pool, usize, alignment, false);

	return (p);
}

JEMALLOC_ALWAYS_INLINE_C void *
pool_imemalign_prof(pool_t *pool, size_t alignment, size_t usize,
	prof_thr_cnt_t *cnt)
{
	void *p;

	if ((uintptr_t)cnt != (uintptr_t)1U)
		p = pool_imemalign_prof_sample(pool, alignment, usize, cnt);
	else
		p = pool_ipalloc(pool, usize, alignment, false);
	if (p == NULL)
		return (NULL);
	prof_malloc(p, usize, cnt);

	return (p);
}

JEMALLOC_ATTR(nonnull(1))
static int
pool_imemalign(pool_t *pool, void **memptr, size_t alignment, size_t size,
	size_t min_alignment)
{
	int ret;
	size_t usize;
	void *result;

	assert(min_alignment != 0);

	if (malloc_init()) {
		result = NULL;
		goto label_oom;
	} else {
		if (size == 0)
			size = 1;

		/* Make sure that alignment is a large enough power of 2. */
		if (((alignment - 1) & alignment) != 0
		    || (alignment < min_alignment)) {
			if (config_xmalloc && opt_xmalloc) {
				malloc_write("<jemalloc>: Error allocating pool"
				    " aligned memory: invalid alignment\n");
				abort();
			}
			result = NULL;
			ret = EINVAL;
			goto label_return;
		}

		usize = sa2u(size, alignment);
		if (usize == 0) {
			result = NULL;
			goto label_oom;
		}

		if (config_prof && opt_prof) {
			prof_thr_cnt_t *cnt;

			PROF_ALLOC_PREP(usize, cnt);
			result = pool_imemalign_prof(pool, alignment,
				usize, cnt);
		} else
			result = pool_ipalloc(pool, usize, alignment, false);
		if (result == NULL)
			goto label_oom;
	}

	*memptr = result;
	ret = 0;
label_return:
	if (config_stats && result != NULL) {
		assert(usize == isalloc(result, config_prof));
		thread_allocated_tsd_get()->allocated += usize;
	}
	UTRACE(0, size, result);
	return (ret);
label_oom:
	assert(result == NULL);
	if (config_xmalloc && opt_xmalloc) {
		malloc_write("<jemalloc>: Error allocating pool "
			"aligned memory: out of memory\n");
		abort();
	}
	ret = ENOMEM;
	goto label_return;
}

void *
je_pool_aligned_alloc(pool_t *pool, size_t alignment, size_t size)
{
	void *ret;
	int err;

	if ((err = pool_imemalign(pool, &ret, alignment, size, 1)) != 0) {
		ret = NULL;
		set_errno(err);
	}
	JEMALLOC_VALGRIND_MALLOC(err == 0, ret, isalloc(ret, config_prof),
	    false);
	return (ret);
}

void
je_pool_free(pool_t *pool, void *ptr)
{
	UTRACE(ptr, 0, 0);
	if (ptr != NULL)
		pool_ifree(pool, ptr);
}

void
je_pool_malloc_stats_print(pool_t *pool,
				void (*write_cb)(void *, const char *),
				void *cbopaque, const char *opts)
{

	stats_print(pool, write_cb, cbopaque, opts);
}

void
je_pool_set_alloc_funcs(void *(*malloc_func)(size_t),
				void (*free_func)(void *))
{
	if (malloc_func != NULL && free_func != NULL) {
		malloc_mutex_lock(&pool_base_lock);
		if (pools == NULL) {
			je_base_malloc = malloc_func;
			je_base_free = free_func;
		}
		malloc_mutex_unlock(&pool_base_lock);
	}
}

size_t
je_pool_malloc_usable_size(pool_t *pool, void *ptr)
{
	assert(malloc_initialized || IS_INITIALIZER);
	malloc_thread_init();

	if (config_ivsalloc) {
		/* Return 0 if ptr is not within a chunk managed by jemalloc. */
		if (rtree_get(pool->chunks_rtree, (uintptr_t)CHUNK_ADDR2BASE(ptr)) == 0)
			return 0;
	}

	return (ptr != NULL) ? pool_isalloc(pool, ptr, config_prof) : 0;
}

JEMALLOC_ALWAYS_INLINE_C void *
imallocx(size_t usize, size_t alignment, bool zero, bool try_tcache,
    arena_t *arena)
{

	assert(usize == ((alignment == 0) ? s2u(usize) : sa2u(usize,
	    alignment)));

	if (alignment != 0)
		return (ipalloct(usize, alignment, zero, try_tcache, arena));
	else if (zero)
		return (icalloct(usize, try_tcache, arena));
	else
		return (imalloct(usize, try_tcache, arena));
}

static void *
imallocx_prof_sample(size_t usize, size_t alignment, bool zero, bool try_tcache,
    arena_t *arena, prof_thr_cnt_t *cnt)
{
	void *p;

	if (cnt == NULL)
		return (NULL);
	if (usize <= SMALL_MAXCLASS) {
		size_t usize_promoted = (alignment == 0) ?
		    s2u(SMALL_MAXCLASS+1) : sa2u(SMALL_MAXCLASS+1, alignment);
		assert(usize_promoted != 0);
		p = imallocx(usize_promoted, alignment, zero, try_tcache,
		    arena);
		if (p == NULL)
			return (NULL);
		arena_prof_promoted(p, usize);
	} else
		p = imallocx(usize, alignment, zero, try_tcache, arena);

	return (p);
}

JEMALLOC_ALWAYS_INLINE_C void *
imallocx_prof(size_t usize, size_t alignment, bool zero, bool try_tcache,
    arena_t *arena, prof_thr_cnt_t *cnt)
{
	void *p;

	if ((uintptr_t)cnt != (uintptr_t)1U) {
		p = imallocx_prof_sample(usize, alignment, zero, try_tcache,
		    arena, cnt);
	} else
		p = imallocx(usize, alignment, zero, try_tcache, arena);
	if (p == NULL)
		return (NULL);
	prof_malloc(p, usize, cnt);

	return (p);
}

void *
je_mallocx(size_t size, int flags)
{
	void *p;
	size_t usize;
	size_t alignment = (ZU(1) << (flags & MALLOCX_LG_ALIGN_MASK)
	    & (SIZE_T_MAX-1));
	bool zero = flags & MALLOCX_ZERO;
	unsigned arena_ind = ((unsigned)(flags >> 8)) - 1;
	pool_t *pool = &base_pool;
	arena_t dummy_arena;
	DUMMY_ARENA_INITIALIZE(dummy_arena, pool);
	arena_t *arena;
	bool try_tcache;

	assert(size != 0);

	if (malloc_init_base_pool())
		goto label_oom;

	if (arena_ind != UINT_MAX) {
		malloc_rwlock_rdlock(&pool->arenas_lock);
		arena = pool->arenas[arena_ind];
		malloc_rwlock_unlock(&pool->arenas_lock);
		try_tcache = false;
	} else {
		arena = &dummy_arena;
		try_tcache = true;
	}

	usize = (alignment == 0) ? s2u(size) : sa2u(size, alignment);
	assert(usize != 0);

	if (config_prof && opt_prof) {
		prof_thr_cnt_t *cnt;

		PROF_ALLOC_PREP(usize, cnt);
		p = imallocx_prof(usize, alignment, zero, try_tcache, arena,
		    cnt);
	} else
		p = imallocx(usize, alignment, zero, try_tcache, arena);
	if (p == NULL)
		goto label_oom;

	if (config_stats) {
		assert(usize == isalloc(p, config_prof));
		thread_allocated_tsd_get()->allocated += usize;
	}
	UTRACE(0, size, p);
	JEMALLOC_VALGRIND_MALLOC(true, p, usize, zero);
	return (p);
label_oom:
	if (config_xmalloc && opt_xmalloc) {
		malloc_write("<jemalloc>: Error in mallocx(): out of memory\n");
		abort();
	}
	UTRACE(0, size, 0);
	return (NULL);
}

static void *
irallocx_prof_sample(void *oldptr, size_t size, size_t alignment, size_t usize,
    bool zero, bool try_tcache_alloc, bool try_tcache_dalloc, arena_t *arena,
    prof_thr_cnt_t *cnt)
{
	void *p;

	if (cnt == NULL)
		return (NULL);
	if (usize <= SMALL_MAXCLASS) {
		p = iralloct(oldptr, SMALL_MAXCLASS+1, (SMALL_MAXCLASS+1 >=
		    size) ? 0 : size - (SMALL_MAXCLASS+1), alignment, zero,
		    try_tcache_alloc, try_tcache_dalloc, arena);
		if (p == NULL)
			return (NULL);
		arena_prof_promoted(p, usize);
	} else {
		p = iralloct(oldptr, size, 0, alignment, zero,
		    try_tcache_alloc, try_tcache_dalloc, arena);
	}

	return (p);
}

JEMALLOC_ALWAYS_INLINE_C void *
irallocx_prof(void *oldptr, size_t old_usize, size_t size, size_t alignment,
    size_t *usize, bool zero, bool try_tcache_alloc, bool try_tcache_dalloc,
    arena_t *arena, prof_thr_cnt_t *cnt)
{
	void *p;
	prof_ctx_t *old_ctx;

	old_ctx = prof_ctx_get(oldptr);
	if ((uintptr_t)cnt != (uintptr_t)1U)
		p = irallocx_prof_sample(oldptr, size, alignment, *usize, zero,
		    try_tcache_alloc, try_tcache_dalloc, arena, cnt);
	else {
		p = iralloct(oldptr, size, 0, alignment, zero,
		    try_tcache_alloc, try_tcache_dalloc, arena);
	}
	if (p == NULL)
		return (NULL);

	if (p == oldptr && alignment != 0) {
		/*
		 * The allocation did not move, so it is possible that the size
		 * class is smaller than would guarantee the requested
		 * alignment, and that the alignment constraint was
		 * serendipitously satisfied.  Additionally, old_usize may not
		 * be the same as the current usize because of in-place large
		 * reallocation.  Therefore, query the actual value of usize.
		 */
		*usize = isalloc(p, config_prof);
	}
	prof_realloc(p, *usize, cnt, old_usize, old_ctx);

	return (p);
}

void *
je_rallocx(void *ptr, size_t size, int flags)
{
	void *p;
	size_t usize, old_usize;
	UNUSED size_t old_rzsize JEMALLOC_CC_SILENCE_INIT(0);
	size_t alignment = (ZU(1) << (flags & MALLOCX_LG_ALIGN_MASK)
	    & (SIZE_T_MAX-1));
	bool zero = flags & MALLOCX_ZERO;
	unsigned arena_ind = ((unsigned)(flags >> 8)) - 1;
	pool_t *pool = &base_pool;
	arena_t dummy_arena;
	DUMMY_ARENA_INITIALIZE(dummy_arena, pool);
	bool try_tcache_alloc, try_tcache_dalloc;
	arena_t *arena;

	assert(ptr != NULL);
	assert(size != 0);
	assert(malloc_initialized || IS_INITIALIZER);
	malloc_thread_init();

	if (arena_ind != UINT_MAX) {
		arena_chunk_t *chunk;
		try_tcache_alloc = false;
		chunk = (arena_chunk_t *)CHUNK_ADDR2BASE(ptr);
		try_tcache_dalloc = (chunk == ptr || chunk->arena !=
		    pool->arenas[arena_ind]);
		arena = pool->arenas[arena_ind];
	} else {
		try_tcache_alloc = true;
		try_tcache_dalloc = true;
		arena = &dummy_arena;
	}

	if ((config_prof && opt_prof) || config_stats ||
	    (config_valgrind && in_valgrind))
		old_usize = isalloc(ptr, config_prof);
	if (config_valgrind && in_valgrind)
		old_rzsize = u2rz(old_usize);

	if (config_prof && opt_prof) {
		prof_thr_cnt_t *cnt;

		usize = (alignment == 0) ? s2u(size) : sa2u(size, alignment);
		assert(usize != 0);
		PROF_ALLOC_PREP(usize, cnt);
		p = irallocx_prof(ptr, old_usize, size, alignment, &usize, zero,
		    try_tcache_alloc, try_tcache_dalloc, arena, cnt);
		if (p == NULL)
			goto label_oom;
	} else {
		p = iralloct(ptr, size, 0, alignment, zero, try_tcache_alloc,
		    try_tcache_dalloc, arena);
		if (p == NULL)
			goto label_oom;
		if (config_stats || (config_valgrind && in_valgrind))
			usize = isalloc(p, config_prof);
	}

	if (config_stats) {
		thread_allocated_t *ta;
		ta = thread_allocated_tsd_get();
		ta->allocated += usize;
		ta->deallocated += old_usize;
	}
	UTRACE(ptr, size, p);
	JEMALLOC_VALGRIND_REALLOC(true, p, usize, false, ptr, old_usize,
	    old_rzsize, false, zero);
	return (p);
label_oom:
	if (config_xmalloc && opt_xmalloc) {
		malloc_write("<jemalloc>: Error in rallocx(): out of memory\n");
		abort();
	}
	UTRACE(ptr, size, 0);
	return (NULL);
}

JEMALLOC_ALWAYS_INLINE_C size_t
ixallocx_helper(void *ptr, size_t old_usize, size_t size, size_t extra,
    size_t alignment, bool zero, arena_t *arena)
{
	size_t usize;

	if (ixalloc(ptr, size, extra, alignment, zero))
		return (old_usize);
	usize = isalloc(ptr, config_prof);

	return (usize);
}

static size_t
ixallocx_prof_sample(void *ptr, size_t old_usize, size_t size, size_t extra,
    size_t alignment, size_t max_usize, bool zero, arena_t *arena,
    prof_thr_cnt_t *cnt)
{
	size_t usize;

	if (cnt == NULL)
		return (old_usize);
	/* Use minimum usize to determine whether promotion may happen. */
	if (((alignment == 0) ? s2u(size) : sa2u(size, alignment)) <=
	    SMALL_MAXCLASS) {
		if (ixalloc(ptr, SMALL_MAXCLASS+1, (SMALL_MAXCLASS+1 >=
		    size+extra) ? 0 : size+extra - (SMALL_MAXCLASS+1),
		    alignment, zero))
			return (old_usize);
		usize = isalloc(ptr, config_prof);
		if (max_usize < PAGE)
			arena_prof_promoted(ptr, usize);
	} else {
		usize = ixallocx_helper(ptr, old_usize, size, extra, alignment,
		    zero, arena);
	}

	return (usize);
}

JEMALLOC_ALWAYS_INLINE_C size_t
ixallocx_prof(void *ptr, size_t old_usize, size_t size, size_t extra,
    size_t alignment, size_t max_usize, bool zero, arena_t *arena,
    prof_thr_cnt_t *cnt)
{
	size_t usize;
	prof_ctx_t *old_ctx;

	old_ctx = prof_ctx_get(ptr);
	if ((uintptr_t)cnt != (uintptr_t)1U) {
		usize = ixallocx_prof_sample(ptr, old_usize, size, extra,
		    alignment, zero, max_usize, arena, cnt);
	} else {
		usize = ixallocx_helper(ptr, old_usize, size, extra, alignment,
		    zero, arena);
	}
	if (usize == old_usize)
		return (usize);
	prof_realloc(ptr, usize, cnt, old_usize, old_ctx);

	return (usize);
}

size_t
je_xallocx(void *ptr, size_t size, size_t extra, int flags)
{
	size_t usize, old_usize;
	UNUSED size_t old_rzsize JEMALLOC_CC_SILENCE_INIT(0);
	size_t alignment = (ZU(1) << (flags & MALLOCX_LG_ALIGN_MASK)
	    & (SIZE_T_MAX-1));
	bool zero = flags & MALLOCX_ZERO;
	unsigned arena_ind = ((unsigned)(flags >> 8)) - 1;
	pool_t *pool = &base_pool;
	arena_t dummy_arena;
	DUMMY_ARENA_INITIALIZE(dummy_arena, pool);
	arena_t *arena;

	assert(ptr != NULL);
	assert(size != 0);
	assert(SIZE_T_MAX - size >= extra);
	assert(malloc_initialized || IS_INITIALIZER);
	malloc_thread_init();

	if (arena_ind != UINT_MAX)
		arena = pool->arenas[arena_ind];
	else
		arena = &dummy_arena;

	old_usize = isalloc(ptr, config_prof);
	if (config_valgrind && in_valgrind)
		old_rzsize = u2rz(old_usize);

	if (config_prof && opt_prof) {
		prof_thr_cnt_t *cnt;
		/*
		 * usize isn't knowable before ixalloc() returns when extra is
		 * non-zero.  Therefore, compute its maximum possible value and
		 * use that in PROF_ALLOC_PREP() to decide whether to capture a
		 * backtrace.  prof_realloc() will use the actual usize to
		 * decide whether to sample.
		 */
		size_t max_usize = (alignment == 0) ? s2u(size+extra) :
		    sa2u(size+extra, alignment);
		PROF_ALLOC_PREP(max_usize, cnt);
		usize = ixallocx_prof(ptr, old_usize, size, extra, alignment,
		    max_usize, zero, arena, cnt);
	} else {
		usize = ixallocx_helper(ptr, old_usize, size, extra, alignment,
		    zero, arena);
	}
	if (usize == old_usize)
		goto label_not_resized;

	if (config_stats) {
		thread_allocated_t *ta;
		ta = thread_allocated_tsd_get();
		ta->allocated += usize;
		ta->deallocated += old_usize;
	}
	JEMALLOC_VALGRIND_REALLOC(false, ptr, usize, false, ptr, old_usize,
	    old_rzsize, false, zero);
label_not_resized:
	UTRACE(ptr, size, ptr);
	return (usize);
}

size_t
je_sallocx(const void *ptr, int flags)
{
	size_t usize;

	assert(malloc_initialized || IS_INITIALIZER);
	malloc_thread_init();

	if (config_ivsalloc)
		usize = ivsalloc(ptr, config_prof);
	else {
		assert(ptr != NULL);
		usize = isalloc(ptr, config_prof);
	}

	return (usize);
}

void
je_dallocx(void *ptr, int flags)
{
	size_t usize;
	UNUSED size_t rzsize JEMALLOC_CC_SILENCE_INIT(0);
	unsigned arena_ind = ((unsigned)(flags >> 8)) - 1;
	pool_t *pool = &base_pool;
	bool try_tcache;

	assert(ptr != NULL);
	assert(malloc_initialized || IS_INITIALIZER);

	if (arena_ind != UINT_MAX) {
		arena_chunk_t *chunk = (arena_chunk_t *)CHUNK_ADDR2BASE(ptr);
		try_tcache = (chunk == ptr || chunk->arena !=
		    pool->arenas[arena_ind]);
	} else
		try_tcache = true;

	UTRACE(ptr, 0, 0);
	if (config_stats || config_valgrind)
		usize = isalloc(ptr, config_prof);
	if (config_prof && opt_prof) {
		if (config_stats == false && config_valgrind == false)
			usize = isalloc(ptr, config_prof);
		prof_free(ptr, usize);
	}
	if (config_stats)
		thread_allocated_tsd_get()->deallocated += usize;
	if (config_valgrind && in_valgrind)
		rzsize = p2rz(ptr);
	iqalloct(ptr, try_tcache);
	JEMALLOC_VALGRIND_FREE(ptr, rzsize);
}

size_t
je_nallocx(size_t size, int flags)
{
	size_t usize;
	size_t alignment = (ZU(1) << (flags & MALLOCX_LG_ALIGN_MASK)
	    & (SIZE_T_MAX-1));

	assert(size != 0);

	if (malloc_init_base_pool())
		return (0);

	usize = (alignment == 0) ? s2u(size) : sa2u(size, alignment);
	assert(usize != 0);
	return (usize);
}

int
je_mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp,
    size_t newlen)
{
	return (ctl_byname(name, oldp, oldlenp, newp, newlen));
}

int
je_mallctlnametomib(const char *name, size_t *mibp, size_t *miblenp)
{
	return (ctl_nametomib(name, mibp, miblenp));
}

int
je_mallctlbymib(const size_t *mib, size_t miblen, void *oldp, size_t *oldlenp,
  void *newp, size_t newlen)
{
	return (ctl_bymib(mib, miblen, oldp, oldlenp, newp, newlen));
}

int
je_navsnprintf(char *str, size_t size, const char *format, va_list ap)
{
	return malloc_vsnprintf(str, size, format, ap);
}

void
je_malloc_stats_print(void (*write_cb)(void *, const char *), void *cbopaque,
    const char *opts)
{
	stats_print(&base_pool, write_cb, cbopaque, opts);
}

size_t
je_malloc_usable_size(JEMALLOC_USABLE_SIZE_CONST void *ptr)
{
	size_t ret;

	assert(malloc_initialized || IS_INITIALIZER);
	malloc_thread_init();

	if (config_ivsalloc)
		ret = ivsalloc(ptr, config_prof);
	else
		ret = (ptr != NULL) ? isalloc(ptr, config_prof) : 0;

	return (ret);
}

/*
 * End non-standard functions.
 */
/******************************************************************************/
/*
 * The following functions are used by threading libraries for protection of
 * malloc during fork().
 */

/*
 * If an application creates a thread before doing any allocation in the main
 * thread, then calls fork(2) in the main thread followed by memory allocation
 * in the child process, a race can occur that results in deadlock within the
 * child: the main thread may have forked while the created thread had
 * partially initialized the allocator.  Ordinarily jemalloc prevents
 * fork/malloc races via the following functions it registers during
 * initialization using pthread_atfork(), but of course that does no good if
 * the allocator isn't fully initialized at fork time.  The following library
 * constructor is a partial solution to this problem.  It may still possible to
 * trigger the deadlock described above, but doing so would involve forking via
 * a library constructor that runs before jemalloc's runs.
 */
JEMALLOC_ATTR(constructor)
static void
jemalloc_constructor(void)
{

	malloc_init();
}

JEMALLOC_ATTR(destructor)
static void
jemalloc_destructor(void)
{

	tcache_thread_cleanup(tcache_tsd_get());
	arenas_cleanup(arenas_tsd_get());
}

#define FOREACH_POOL(func)					\
do {								\
	unsigned i;						\
	for (i = 0; i < npools; i++) {			\
		if (pools[i] != NULL)				\
			(func)(pools[i]);			\
	}							\
} while(0)

#ifndef JEMALLOC_MUTEX_INIT_CB
void
jemalloc_prefork(void)
#else
JEMALLOC_EXPORT void
_malloc_prefork(void)
#endif
{
	unsigned i, j;
	pool_t *pool;

#ifdef JEMALLOC_MUTEX_INIT_CB
	if (malloc_initialized == false)
		return;
#endif
	assert(malloc_initialized);

	/* Acquire all mutexes in a safe order. */
	ctl_prefork();
	prof_prefork();
	pool_prefork();

	for (i = 0; i < npools; i++) {
		pool = pools[i];
		if (pool != NULL) {
			malloc_rwlock_prefork(&pool->arenas_lock);
			for (j = 0; j < pool->narenas_total; j++) {
				if (pool->arenas[j] != NULL)
					arena_prefork(pool->arenas[j]);
			}
		}
	}

	FOREACH_POOL(chunk_prefork);
	chunk_dss_prefork();

	FOREACH_POOL(base_prefork);

	FOREACH_POOL(huge_prefork);
}

#ifndef JEMALLOC_MUTEX_INIT_CB
void
jemalloc_postfork_parent(void)
#else
JEMALLOC_EXPORT void
_malloc_postfork(void)
#endif
{
	unsigned i, j;
	pool_t *pool;

#ifdef JEMALLOC_MUTEX_INIT_CB
	if (malloc_initialized == false)
		return;
#endif
	assert(malloc_initialized);

	/* Release all mutexes, now that fork() has completed. */
	FOREACH_POOL(huge_postfork_parent);

	FOREACH_POOL(base_postfork_parent);

	chunk_dss_postfork_parent();
	FOREACH_POOL(chunk_postfork_parent);

	for (i = 0; i < npools; i++) {
		pool = pools[i];
		if (pool != NULL) {
			for (j = 0; j < pool->narenas_total; j++) {
				if (pool->arenas[j] != NULL)
					arena_postfork_parent(pool->arenas[j]);
			}
			malloc_rwlock_postfork_parent(&pool->arenas_lock);
		}
	}

	pool_postfork_parent();
	prof_postfork_parent();
	ctl_postfork_parent();
}

void
jemalloc_postfork_child(void)
{
	unsigned i, j;
	pool_t *pool;

	assert(malloc_initialized);

	/* Release all mutexes, now that fork() has completed. */
	FOREACH_POOL(huge_postfork_child);

	FOREACH_POOL(base_postfork_child);

	chunk_dss_postfork_child();
	FOREACH_POOL(chunk_postfork_child);

	for (i = 0; i < npools; i++) {
		pool = pools[i];
		if (pool != NULL) {
			for (j = 0; j < pool->narenas_total; j++) {
				if (pool->arenas[j] != NULL)
					arena_postfork_child(pool->arenas[j]);
			}
			malloc_rwlock_postfork_child(&pool->arenas_lock);
		}
	}

	pool_postfork_child();
	prof_postfork_child();
	ctl_postfork_child();
}

/******************************************************************************/
/*
 * The following functions are used for TLS allocation/deallocation in static
 * binaries on FreeBSD.  The primary difference between these and i[mcd]alloc()
 * is that these avoid accessing TLS variables.
 */

static void *
a0alloc(size_t size, bool zero)
{

	if (malloc_init_base_pool())
		return (NULL);

	if (size == 0)
		size = 1;

	if (size <= arena_maxclass)
		return (arena_malloc(base_pool.arenas[0], size, zero, false));
	else
		return (huge_malloc(NULL, size, zero));
}

void *
a0malloc(size_t size)
{

	return (a0alloc(size, false));
}

void *
a0calloc(size_t num, size_t size)
{

	return (a0alloc(num * size, true));
}

void
a0free(void *ptr)
{
	arena_chunk_t *chunk;

	if (ptr == NULL)
		return;

	chunk = (arena_chunk_t *)CHUNK_ADDR2BASE(ptr);
	if (chunk != ptr)
		arena_dalloc(chunk, ptr, false);
	else
		huge_dalloc(&base_pool, ptr);
}

/******************************************************************************/
