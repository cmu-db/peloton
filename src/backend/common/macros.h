//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// macros.h
//
// Identification: src/backend/common/macros.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <assert.h>
#include <stdexcept>

namespace peloton {

//===--------------------------------------------------------------------===//
// branch predictor hints
//===--------------------------------------------------------------------===//

#define likely_branch(x)   __builtin_expect(!!(x), 1)
#define unlikely_branch(x) __builtin_expect(!!(x), 0)

//===--------------------------------------------------------------------===//
// attributes
//===--------------------------------------------------------------------===//

#define NEVER_INLINE  __attribute__((noinline))
#define ALWAYS_INLINE __attribute__((always_inline))
#define UNUSED_ATTRIBUTE __attribute__((unused))

//===--------------------------------------------------------------------===//
// memcpy
//===--------------------------------------------------------------------===//

#define USE_BUILTIN_MEMFUNCS

#ifdef USE_BUILTIN_MEMFUNCS
#define NDB_MEMCPY __builtin_memcpy
#define NDB_MEMSET __builtin_memset
#else
#define NDB_MEMCPY memcpy
#define NDB_MEMSET memset
#endif

//===--------------------------------------------------------------------===//
// packed
//===--------------------------------------------------------------------===//

#define __XCONCAT2(a, b) a ## b
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define CACHE_PADOUT  \
    char __XCONCAT(__padout, __COUNTER__)[0] __attribute__((aligned(CACHELINE_SIZE)))
#define PACKED __attribute__((packed))

//===--------------------------------------------------------------------===//
// invariants
//===--------------------------------------------------------------------===//

#ifdef CHECK_INVARIANTS
  #define INVARIANT(expr) ALWAYS_ASSERT(expr)
#else
  #define INVARIANT(expr) ((void)0)
#endif /* CHECK_INVARIANTS */

//===--------------------------------------------------------------------===//
// unimplemented
//===--------------------------------------------------------------------===//

// throw exception after the ALWAYS_ASSERT(), so that GCC knows
// we'll never return
#define NDB_UNIMPLEMENTED(what) \
  do { \
    ALWAYS_ASSERT(false); \
    throw ::std::runtime_error(what); \
  } while (0)

//===--------------------------------------------------------------------===//
// ALWAYS_ASSERT
//===--------------------------------------------------------------------===//

#ifdef NDEBUG
  #define ALWAYS_ASSERT(expr) (likely_branch((expr)) ? (void)0 : abort())
#else
  #define ALWAYS_ASSERT(expr) ALWAYS_ASSERT((expr))
#endif /* NDEBUG */

//===--------------------------------------------------------------------===//
// override
//===--------------------------------------------------------------------===//

#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 7)
#define GCC_AT_LEAST_47 1
#else
#define GCC_AT_LEAST_47 0
#endif

// g++-4.6 does not support override
#if GCC_AT_LEAST_47
#define OVERRIDE override
#else
#define OVERRIDE
#endif

}  // End peloton namespace
