//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// platform.h
//
// Identification: src/include/common/platform.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <thread>
#include <atomic>

#include <pthread.h>
#include <immintrin.h>

#include "common/macros.h"

//===--------------------------------------------------------------------===//
// Platform-Specific Code
//===--------------------------------------------------------------------===//

namespace peloton {

template <typename T>
inline bool atomic_cas(T *object, T old_value, T new_value) {
  return __sync_bool_compare_and_swap(object, old_value, new_value);
}

#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")

//===--------------------------------------------------------------------===//
// Alignment
//===--------------------------------------------------------------------===//

#define CACHELINE_SIZE 64  // XXX: don't assume x86

// some helpers for cacheline alignment
#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))

//===--------------------------------------------------------------------===//
// Count the number of leading zeroes in a given 64-bit unsigned number
//===--------------------------------------------------------------------===//
static inline uint64_t CountLeadingZeroes(uint64_t i) {
#if defined __GNUC__ || defined __clang__
  return __builtin_clzl(i);
#else
#error get a better compiler
#endif
}

//===--------------------------------------------------------------------===//
// Find the next power of two higher than the provided value
//===--------------------------------------------------------------------===//
static inline uint64_t NextPowerOf2(uint64_t n) {
#if defined __GNUC__ || defined __clang__
  PL_ASSERT(n > 0);
  return 1ul << (64 - CountLeadingZeroes(n - 1));
#else
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  return ++n;
#endif
}

}  // namespace peloton
