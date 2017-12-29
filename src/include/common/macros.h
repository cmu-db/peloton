//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// macros.h
//
// Identification: src/include/common/macros.h
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

#define likely_branch(x) __builtin_expect(!!(x), 1)
#define unlikely_branch(x) __builtin_expect(!!(x), 0)

//===--------------------------------------------------------------------===//
// attributes
//===--------------------------------------------------------------------===//

#define NEVER_INLINE __attribute__((noinline))
#define ALWAYS_INLINE __attribute__((always_inline))
#define UNUSED_ATTRIBUTE __attribute__((unused))
#define PACKED __attribute__((packed))

//===--------------------------------------------------------------------===//
// memfuncs
//===--------------------------------------------------------------------===//

#define USE_BUILTIN_MEMFUNCS

#ifdef USE_BUILTIN_MEMFUNCS
#define PL_MEMCPY __builtin_memcpy
#define PL_MEMSET __builtin_memset
#else
#define PL_MEMCPY memcpy
#define PL_MEMSET memset
#endif

//===--------------------------------------------------------------------===//
// packed
//===--------------------------------------------------------------------===//

#define __XCONCAT2(a, b) a##b
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define CACHE_PADOUT                       \
  char __XCONCAT(__padout, __COUNTER__)[0] \
      __attribute__((aligned(CACHELINE_SIZE)))
#define PACKED __attribute__((packed))

//===--------------------------------------------------------------------===//
// invariants
//===--------------------------------------------------------------------===//

#ifdef CHECK_INVARIANTS
#define INVARIANT(expr) PL_ASSERT(expr)
#else
#define INVARIANT(expr) ((void)0)
#endif /* CHECK_INVARIANTS */

//===--------------------------------------------------------------------===//
// unimplemented
//===--------------------------------------------------------------------===//

// throw exception after the assert(), so that GCC knows
// we'll never return
#define PL_UNIMPLEMENTED(what)        \
  do {                                \
    PL_ASSERT(false);                 \
    throw ::std::runtime_error(what); \
  } while (0)

//===--------------------------------------------------------------------===//
// ALWAYS_ASSERT
//===--------------------------------------------------------------------===//

#ifdef NDEBUG
#define PL_ASSERT(expr) ((void)0)
#else
#define PL_ASSERT(expr) assert((expr))
#endif /* NDEBUG */

#ifdef CHECK_INVARIANTS
#define INVARIANT(expr) PL_ASSERT(expr)
#else
#define INVARIANT(expr) ((void)0)
#endif /* CHECK_INVARIANTS */

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

//===--------------------------------------------------------------------===//
// Port to OSX
//===---------------------------
#ifdef __APPLE__
#define off64_t off_t
#define MAP_ANONYMOUS MAP_ANON
#endif

//===--------------------------------------------------------------------===//
// utils
//===--------------------------------------------------------------------===//

#define ARRAY_NELEMS(a) (sizeof(a) / sizeof((a)[0]))

//===----------------------------------------------------------------------===//
// Handy macros to hide move/copy class constructors
//===----------------------------------------------------------------------===//

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)     \
  cname(const cname &) = delete; \
  cname &operator=(const cname &) = delete;

#define DISALLOW_MOVE(cname) \
  cname(cname &&) = delete;  \
  cname &operator=(cname &&) = delete;

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

//===----------------------------------------------------------------------===//
// LLVM version checking macros
//===----------------------------------------------------------------------===//

#define LLVM_VERSION_GE(major, minor) \
  (LLVM_VERSION_MAJOR > (major) ||    \
   (LLVM_VERSION_MAJOR == (major) && LLVM_VERSION_MINOR >= (minor)))

#define LLVM_VERSION_EQ(major, minor) \
  (LLVM_VERSION_MAJOR == (major) && LLVM_VERSION_MINOR == (minor))

//===----------------------------------------------------------------------===//
// switch statements
//===----------------------------------------------------------------------===//

#if defined __clang__
#define PELOTON_FALLTHROUGH [[clang::fallthrough]]
#elif defined __GNUC__ && __GNUC__ >= 7
#define PELOTON_FALLTHROUGH [[fallthrough]]
#else
#define PELOTON_FALLTHROUGH
#endif


}  // namespace peloton