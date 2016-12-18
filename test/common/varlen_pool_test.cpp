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
#include "type/varlen_pool.h"
#include "gtest/gtest.h"
#include "common/harness.h"

namespace peloton {
namespace test {

class VarlenPoolTests : public PelotonTest {};

#define N 10
#define M 1000
#define R 1
#define RANDOM(a) (rand() % a) // Generate a random number in [0, a)

const size_t str_len = 1000; // test string length

using namespace peloton::common;

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
TEST_F(VarlenPoolTests, AllocateOnceTest) {
  type::VarlenPool *pool = new type::VarlenPool(peloton::BACKEND_TYPE_MM);
  void *p = nullptr;
  size_t size;
  size_t total_size = 0;
  size = 40;

  total_size += get_align(size);
  p = pool->Allocate(size);
  EXPECT_EQ(total_size, pool->GetTotalAllocatedSpace());
  EXPECT_TRUE(p != nullptr);

  total_size -= get_align(size);
  pool->Free(p);
  EXPECT_EQ(total_size, pool->GetTotalAllocatedSpace());

  delete pool;
}

// Allocate and free N blocks from each buffer list
TEST_F(VarlenPoolTests, AllocateTest) {
  std::srand(std::time(0));
  type::VarlenPool *pool = new type::VarlenPool(peloton::BACKEND_TYPE_MM);
  char *p[MAX_LIST_NUM][N];
  char *test_str = new char[str_len];
  size_t size, block_size;
  size_t total_size = 0;

  for (size_t i = 0; i < MAX_LIST_NUM; i++) {
    for (size_t j = 0; j < N; j++) {
      p[i][j] = nullptr;
    }
  }

  // Generate a random test string
  for (size_t i = 0; i < str_len; i++)
    test_str[i] = RANDOM(26) + 'a';

  // Allocate N blocks from each buffer list
  for (size_t i = 0; i < MAX_LIST_NUM; i++) {
    block_size = 1 << (i + 4);
    for (size_t j = 0; j < N; j++) {
      size = j % (block_size >> 1) + (block_size >> 1);
      p[i][j] = (char*) pool->Allocate(size);
      for (size_t k = 0; k < size; k++) {
        p[i][j][k] = test_str[(i + j * k) % str_len];
      }
      EXPECT_TRUE(p[i][j] != nullptr);
      total_size += get_align(size);
      EXPECT_EQ(total_size, pool->GetTotalAllocatedSpace());
    }

    // Free and reallocate some of the pointers
    for (size_t j = 0; j < N; j += 2) {
      size = j % (block_size >> 1) + (block_size >> 1);
      pool->Free(p[i][j]);
      total_size -= get_align(size);
      EXPECT_EQ(total_size, pool->GetTotalAllocatedSpace());
    }
    for (size_t j = 0; j < N; j += 2) {
      size = j % (block_size >> 1) + (block_size >> 1);
      p[i][j] = (char*) pool->Allocate(size);
      for (size_t k = 0; k < size; k++) {
        p[i][j][k] = test_str[(i * j * k + 1) % str_len];
      }
      EXPECT_TRUE(p[i][j] != nullptr);
      total_size += get_align(size);
      EXPECT_EQ(total_size, pool->GetTotalAllocatedSpace());
    }

    // Compare the remaining strings with the random test string
    for (size_t j = 1; j < N; j += 2) {
      size = j % (block_size >> 1) + (block_size >> 1);
      for (size_t k = 0; k < size; k++) {
        EXPECT_EQ(p[i][j][k], test_str[(i + j * k) % str_len]);
      }
    }

    // Free all the pointers
    for (size_t j = 0; j < N; j++) {
      size = j % (block_size >> 1) + (block_size >> 1);
      pool->Free(p[i][j]);
      total_size -= get_align(size);
      EXPECT_EQ(total_size, pool->GetTotalAllocatedSpace());
    }
  }

  // All the pointers have been freed
  EXPECT_EQ(0, pool->GetTotalAllocatedSpace());

  // Test compaction
  for (size_t i = 0; i < LARGE_LIST_ID; i++)
    EXPECT_TRUE(MAX_EMPTY_NUM >= pool->empty_cnt_[i]);
  EXPECT_EQ(0, pool->empty_cnt_[LARGE_LIST_ID]);

  delete[] test_str;
  delete pool;
}

// Random allocation and free
TEST_F(VarlenPoolTests, RandomTest) {
  std::srand(std::time(0));
  type::VarlenPool *pool = new type::VarlenPool(peloton::BACKEND_TYPE_MM);
  char *p[M] = {nullptr};
  char *test_str = new char[BUFFER_SIZE << 1];
  size_t size[M] = {};
  size_t total_size = 0;

  // Generate a random test string
  for (size_t i = 0; i < BUFFER_SIZE << 1; i++)
    test_str[i] = RANDOM(26) + 'a';

  // Repeat R times
  for (size_t i = 0; i < R; i++) {
    // Allocate all the pointers
    for (size_t j = 0; j < M; j++) {
      if (p[j] != nullptr)
        continue;
      size[j] = RANDOM(16 << i) + 1;
      p[j] = (char *) pool->Allocate(size[j]);
      EXPECT_TRUE(p[j] != nullptr);
      for (size_t k = 0; k < size[j]; k++) {
        p[j][k] = test_str[(j * k) % str_len];
      }
      total_size += get_align(size[j]);
      EXPECT_EQ(total_size, pool->GetTotalAllocatedSpace());
    }

    // Randomly free some pointers
    for (size_t j = 0; j < M; j++) {
      if (p[j] == nullptr)
        continue;
      if (RANDOM(2) == 0)
        continue;
      pool->Free(p[j]);
      p[j] = nullptr;
      total_size -= get_align(size[j]);
      EXPECT_EQ(total_size, pool->GetTotalAllocatedSpace());
    }

    // Compare the value of pointers with the random test string
    for (size_t j = 0; j < M; j++) {
      if (p[j] == nullptr)
        continue;
      for (size_t k = 0; k < size[j]; k++) {
        EXPECT_EQ(p[j][k], test_str[(j * k) % str_len]);
      }
    }
  }

  // Free all the pointers
  for (size_t j = 0; j < M; j++) {
    if (p[j] == nullptr)
      continue;
    pool->Free(p[j]);
    total_size -= get_align(size[j]);
    EXPECT_EQ(total_size, pool->GetTotalAllocatedSpace());
  }

  // All the pointers have been freed
  EXPECT_EQ(0, pool->GetTotalAllocatedSpace());

  // Test compaction
  for (size_t i = 0; i < LARGE_LIST_ID; i++)
    EXPECT_TRUE(MAX_EMPTY_NUM >= pool->empty_cnt_[i]);
  EXPECT_EQ(0, pool->empty_cnt_[LARGE_LIST_ID]);

  delete[] test_str;
  delete pool;
}

// Allocate and free N/2 blocks from each buffer list
void *thread_all(void *arg) {
  std::srand(std::time(0));
  type::VarlenPool *pool = (type::VarlenPool *) arg;
  char *p[MAX_LIST_NUM][N/2];
  char *test_str = new char[str_len];
  size_t size, block_size;

  for (size_t i = 0; i < MAX_LIST_NUM; i++) {
    for (size_t j = 0; j < N/2; j++) {
      p[i][j] = nullptr;
    }
  }

  // Generate a random test string
  for (size_t i = 0; i < str_len; i++)
    test_str[i] = RANDOM(26) + 'a';

  // Allocate N blocks from each buffer list
  for (size_t i = 0; i < MAX_LIST_NUM; i++) {
    block_size = 1 << (i + 4);
    for (size_t j = 0; j < N/2; j++) {
      size = j % (block_size >> 1) + (block_size >> 1);
      p[i][j] = (char*) pool->Allocate(size);
      for (size_t k = 0; k < size; k++) {
        p[i][j][k] = test_str[(i * j * k) % str_len];
      }
      EXPECT_TRUE(p[i][j] != nullptr);
    }

    // Free and reallocate some of the pointers
    for (size_t j = 0; j < N/2; j += 2) {
      pool->Free(p[i][j]);
    }
    for (size_t j = 0; j < N/2; j += 2) {
      size = j % (block_size >> 1) + (block_size >> 1);
      p[i][j] = (char*) pool->Allocate(size);
      for (size_t k = 0; k < size; k++) {
        p[i][j][k] = test_str[(i * j * k + 1) % str_len];
      }
      EXPECT_TRUE(p[i][j] != nullptr);
    }

    // Compare the remaining strings with the random test string
    for (size_t j = 1; j < N/2; j += 2) {
      size = j % (block_size >> 1) + (block_size >> 1);
      for (size_t k = 0; k < size; k++) {
        EXPECT_EQ(p[i][j][k], test_str[(i * j * k) % str_len]);
      }
    }

    // Free all the pointers
    for (size_t j = 0; j < N/2; j++) {
      pool->Free(p[i][j]);
      p[i][j] = nullptr;
    }
  }

  for (size_t i = 0; i < 1; i++) {
    for (size_t j = 0; j < N/2; j++) {
      EXPECT_TRUE(p[i][j] == nullptr);
    }
  }
  delete[] test_str;
  pthread_exit(NULL);
}

void *thread_random(void *arg) {
  std::srand(std::time(0));
  const size_t m = 1000;
  type::VarlenPool *pool = (type::VarlenPool *) arg;
  char *p[m] = {nullptr};
  char *test_str = new char[str_len];
  size_t size[m] = {};

  // Generate a random test string
  for (size_t i = 0; i < str_len; i++)
    test_str[i] = RANDOM(26) + 'a';

  // Repeat R times
  for (size_t i = 0; i < R; i++) {
    // Allocate all the pointers
    for (size_t j = 0; j < m; j++) {
      if (p[j] != nullptr)
        continue;
      size[j] = RANDOM(16 << i) + 1;
      p[j] = (char *) pool->Allocate(size[j]);
      EXPECT_TRUE(p[j] != nullptr);
      for (size_t k = 0; k < size[j]; k++) {
        p[j][k] = test_str[(j * k) % str_len];
      }
    }

    // Randomly free some pointers
    for (size_t j = 0; j < m; j++) {
      if (p[j] == nullptr)
        continue;
      if (RANDOM(2) == 0)
        continue;
      pool->Free(p[j]);
      p[j] = nullptr;
    }

    // Compare the value of pointers with the random test string
    for (size_t j = 0; j < m; j++) {
      if (p[j] == nullptr)
        continue;
      for (size_t k = 0; k < size[j]; k++) {
        EXPECT_EQ(p[j][k], test_str[(j * k) % str_len]);
      }
    }
  }

  // Free all the pointers
  for (size_t j = 0; j < m; j++) {
    if (p[j] == nullptr)
      continue;
    pool->Free(p[j]);
  }

  delete[] test_str;
  pthread_exit(NULL);
}

TEST_F(VarlenPoolTests, MultithreadTest) {
  type::VarlenPool *pool = new type::VarlenPool(peloton::BACKEND_TYPE_MM);

  pthread_t thread1, thread2;
  UNUSED_ATTRIBUTE int rc;
  rc = pthread_create(&thread1, NULL, thread_all, (void *) pool);
  pthread_join(thread1, NULL);
  rc = pthread_create(&thread2, NULL, thread_all, (void *) pool);
  pthread_join(thread2, NULL);

  // All the pointers have been freed
  EXPECT_EQ(0, pool->GetTotalAllocatedSpace());

  // Test compaction
  for (size_t i = 0; i < LARGE_LIST_ID; i++)
    EXPECT_TRUE(MAX_EMPTY_NUM >= pool->empty_cnt_[i]);
  EXPECT_EQ(0, pool->empty_cnt_[LARGE_LIST_ID]);

  delete pool;
}

TEST_F(VarlenPoolTests, MultithreadRandomTest) {
  type::VarlenPool *pool = new type::VarlenPool(peloton::BACKEND_TYPE_MM);

  pthread_t thread1, thread2;
  UNUSED_ATTRIBUTE int rc;
  rc = pthread_create(&thread1, NULL, thread_random, (void *) pool);
  pthread_join(thread1, NULL);
  rc = pthread_create(&thread2, NULL, thread_random, (void *) pool);
  pthread_join(thread2, NULL);

  // All the pointers have been freed
  EXPECT_EQ(0, pool->GetTotalAllocatedSpace());

  // Test compaction
  for (size_t i = 0; i < LARGE_LIST_ID; i++)
    EXPECT_TRUE(MAX_EMPTY_NUM >= pool->empty_cnt_[i]);
  EXPECT_EQ(0, pool->empty_cnt_[LARGE_LIST_ID]);

  delete pool;
}

}
}
