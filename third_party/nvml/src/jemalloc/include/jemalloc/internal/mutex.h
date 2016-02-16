/******************************************************************************/
#ifdef JEMALLOC_H_TYPES

typedef struct malloc_mutex_s malloc_mutex_t;

#if (defined(_WIN32) || defined(JEMALLOC_OSSPIN) || defined(JEMALLOC_MUTEX_INIT_CB))
typedef malloc_mutex_t malloc_rwlock_t;
#else
typedef struct malloc_rwlock_s malloc_rwlock_t;
#endif

#ifdef _WIN32
#  define MALLOC_MUTEX_INITIALIZER
#elif (defined(JEMALLOC_OSSPIN))
#  define MALLOC_MUTEX_INITIALIZER {0}
#elif (defined(JEMALLOC_MUTEX_INIT_CB))
#  define MALLOC_MUTEX_INITIALIZER {PTHREAD_MUTEX_INITIALIZER, NULL}
#else
#  if (defined(PTHREAD_MUTEX_ADAPTIVE_NP) &&				\
       defined(PTHREAD_ADAPTIVE_MUTEX_INITIALIZER_NP))
#    define MALLOC_MUTEX_TYPE PTHREAD_MUTEX_ADAPTIVE_NP
#    define MALLOC_MUTEX_INITIALIZER {PTHREAD_ADAPTIVE_MUTEX_INITIALIZER_NP}
#  else
#    define MALLOC_MUTEX_TYPE PTHREAD_MUTEX_DEFAULT
#    define MALLOC_MUTEX_INITIALIZER {PTHREAD_MUTEX_INITIALIZER}
#  endif
#endif

#endif /* JEMALLOC_H_TYPES */
/******************************************************************************/

#ifdef JEMALLOC_H_STRUCTS

struct malloc_mutex_s {
#ifdef _WIN32
	CRITICAL_SECTION	lock;
#elif (defined(JEMALLOC_OSSPIN))
	OSSpinLock		lock;
#elif (defined(JEMALLOC_MUTEX_INIT_CB))
	pthread_mutex_t		lock;
	malloc_mutex_t		*postponed_next;
#else
	pthread_mutex_t		lock;
#endif
};

#if (!defined(_WIN32) && !defined(JEMALLOC_OSSPIN) && !defined(JEMALLOC_MUTEX_INIT_CB))
struct malloc_rwlock_s {
	pthread_rwlock_t	lock;
};
#endif

#endif /* JEMALLOC_H_STRUCTS */
/******************************************************************************/
#ifdef JEMALLOC_H_EXTERNS

#ifdef JEMALLOC_LAZY_LOCK
extern bool isthreaded;
#else
#  undef isthreaded /* Undo private_namespace.h definition. */
#  define isthreaded true
#endif

bool	malloc_mutex_init(malloc_mutex_t *mutex);
void	malloc_mutex_prefork(malloc_mutex_t *mutex);
void	malloc_mutex_postfork_parent(malloc_mutex_t *mutex);
void	malloc_mutex_postfork_child(malloc_mutex_t *mutex);
bool	mutex_boot(void);
#if (defined(_WIN32) || defined(JEMALLOC_OSSPIN) || defined(JEMALLOC_MUTEX_INIT_CB))
#define malloc_rwlock_init malloc_mutex_init
#endif
void	malloc_rwlock_prefork(malloc_rwlock_t *rwlock);
void	malloc_rwlock_postfork_parent(malloc_rwlock_t *rwlock);
void	malloc_rwlock_postfork_child(malloc_rwlock_t *rwlock);

#endif /* JEMALLOC_H_EXTERNS */
/******************************************************************************/
#ifdef JEMALLOC_H_INLINES

#ifndef JEMALLOC_ENABLE_INLINE
void	malloc_mutex_lock(malloc_mutex_t *mutex);
void	malloc_mutex_unlock(malloc_mutex_t *mutex);
#if (!defined(_WIN32) && !defined(JEMALLOC_OSSPIN) && !defined(JEMALLOC_MUTEX_INIT_CB))
bool	malloc_rwlock_init(malloc_rwlock_t *rwlock);
#endif
void	malloc_rwlock_rdlock(malloc_rwlock_t *rwlock);
void	malloc_rwlock_wrlock(malloc_rwlock_t *rwlock);
void	malloc_rwlock_unlock(malloc_rwlock_t *rwlock);
void	malloc_rwlock_destroy(malloc_rwlock_t *rwlock);

#endif

#if (defined(JEMALLOC_ENABLE_INLINE) || defined(JEMALLOC_MUTEX_C_))

JEMALLOC_INLINE void
malloc_mutex_lock(malloc_mutex_t *mutex)
{
	if (isthreaded) {
#ifdef _WIN32
		EnterCriticalSection(&mutex->lock);
#elif (defined(JEMALLOC_OSSPIN))
		OSSpinLockLock(&mutex->lock);
#else
		pthread_mutex_lock(&mutex->lock);
#endif
	}
}

JEMALLOC_INLINE void
malloc_mutex_unlock(malloc_mutex_t *mutex)
{
	if (isthreaded) {
#ifdef _WIN32
		LeaveCriticalSection(&mutex->lock);
#elif (defined(JEMALLOC_OSSPIN))
		OSSpinLockUnlock(&mutex->lock);
#else
		pthread_mutex_unlock(&mutex->lock);
#endif
	}
}

JEMALLOC_INLINE void
malloc_rwlock_rdlock(malloc_rwlock_t *rwlock)
{
	if (isthreaded) {
#ifdef _WIN32
		EnterCriticalSection(&rwlock->lock);
#elif (defined(JEMALLOC_OSSPIN))
		OSSpinLockLock(&rwlock->lock);
#elif (defined(JEMALLOC_MUTEX_INIT_CB))
		pthread_mutex_lock(&rwlock->lock);
#else
		pthread_rwlock_rdlock(&rwlock->lock);
#endif
	}
}

JEMALLOC_INLINE void
malloc_rwlock_wrlock(malloc_rwlock_t *rwlock)
{
	if (isthreaded) {
#ifdef _WIN32
		EnterCriticalSection(&rwlock->lock);
#elif (defined(JEMALLOC_OSSPIN))
		OSSpinLockLock(&rwlock->lock);
#elif (defined(JEMALLOC_MUTEX_INIT_CB))
		pthread_mutex_lock(&rwlock->lock);
#else
		pthread_rwlock_wrlock(&rwlock->lock);
#endif
	}
}

JEMALLOC_INLINE void
malloc_rwlock_unlock(malloc_rwlock_t *rwlock)
{
	if (isthreaded) {
#ifdef _WIN32
		LeaveCriticalSection(&rwlock->lock);
#elif (defined(JEMALLOC_OSSPIN))
		OSSpinLockUnlock(&rwlock->lock);
#elif (defined(JEMALLOC_MUTEX_INIT_CB))
		pthread_mutex_unlock(&rwlock->lock);
#else
		pthread_rwlock_unlock(&rwlock->lock);
#endif
	}
}

#if (!defined(_WIN32) && !defined(JEMALLOC_OSSPIN) && !defined(JEMALLOC_MUTEX_INIT_CB))
JEMALLOC_INLINE bool
malloc_rwlock_init(malloc_rwlock_t *rwlock)
{
	if (isthreaded) {
		if (pthread_rwlock_init(&rwlock->lock, NULL) != 0)
			return (true);
	}
	return (false);
}
#endif

JEMALLOC_INLINE void
malloc_rwlock_destroy(malloc_rwlock_t *rwlock)
{
#if (!defined(_WIN32) && !defined(JEMALLOC_OSSPIN) && !defined(JEMALLOC_MUTEX_INIT_CB))
	if (isthreaded) {
		pthread_rwlock_destroy(&rwlock->lock);
	}
#endif
}

#endif

#endif /* JEMALLOC_H_INLINES */
/******************************************************************************/
