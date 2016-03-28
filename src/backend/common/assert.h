//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// assert.h
//
// Identification: src/backend/common/assert.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <cstdio>
#include <cstdlib>

namespace peloton {

// Wraps the standard assert macro to avoids "unused variable" warnings when
// compiled away.
// Inspired by:
// http://cnicholson.net/2009/02/stupid-c-tricks-adventures-in-assert/
// This is not the "default" because it does not conform to the requirements of
// the C standard,
// which requires that the NDEBUG version be ((void) 0).
#ifdef NDEBUG
#define ASSERT(x)    \
  do {               \
    (void)sizeof(x); \
  } while (0)
#else
#define ASSERT(x) assert(x)
#endif

// CHECK is always enabled. May only work with gcc because I stole this from
// assert.h
// TODO: Add our own .cc implementation to avoid duplicating this all over?
// TODO: Use a namespaced function to avoid namespace/class member conflicts?
// TODO: Add unit tests.
#define CHECK(x)                                                          \
  do {                                                                    \
    if (!(x)) {                                                           \
      ::fprintf(stderr, "CHECK failed: %s at %s:%d in function %s\n", #x, \
                __FILE__, __LINE__, __PRETTY_FUNCTION__);                 \
      ::abort();                                                          \
    }                                                                     \
  } while (0)

#define CHECK_M(x, message, args...)                                           \
  do {                                                                         \
    if (!(x)) {                                                                \
      ::fprintf(stderr,                                                        \
                "CHECK failed: %s at %s:%d in function %s\n" message "\n", #x, \
                __FILE__, __LINE__, __PRETTY_FUNCTION__, ##args);              \
      ::abort();                                                               \
    }                                                                          \
  } while (0)

}  // namespace peloton
