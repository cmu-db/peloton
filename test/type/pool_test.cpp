//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen_pool_test.cpp
//
// Identification: test/common/varlen_pool_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <limits.h>
#include <pthread.h>

#include "type/ephemeral_pool.h"
#include "gtest/gtest.h"
#include "common/harness.h"

namespace peloton {
namespace test {

class PoolTests : public PelotonTest {};

#define N 10
#define M 1000
#define R 1
#define RANDOM(a) (rand() % a) // Generate a random number in [0, a)

// disable unused const variable warning for clang
#ifdef __APPLE__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-const-variable"
#endif
const size_t str_len = 1000; // test string length
#ifdef __APPLE__
#pragma clang diagnostic pop
#endif

// Round up to block size
size_t get_align(size_t size) {
  if (size <= 16)
    return 16;
  size_t n = size - 1;
  size_t bits = 0;
  while (n > 0) {
    n = n >> 1;
    bits++;
  }
  return (1 << bits);
}

// Allocate and free once
TEST_F(PoolTests, AllocateOnceTest) {
  type::EphemeralPool *pool = new type::EphemeralPool();
  void *p = nullptr;
  size_t size;
  size = 40;

  p = pool->Allocate(size);
  EXPECT_TRUE(p != nullptr);

  pool->Free(p);

  delete pool;
}

}
}
