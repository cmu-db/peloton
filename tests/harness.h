//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// harness.h
//
// Identification: tests/harness.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <thread>
#include <functional>
#include <iostream>
#include <atomic>

#undef NDEBUG

#include "backend/common/types.h"
#include "backend/common/logger.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <google/protobuf/stubs/common.h>

namespace peloton {

class VarlenPool;

namespace test {

//===--------------------------------------------------------------------===//
// Test Harness (common routines)
//===--------------------------------------------------------------------===//

#define MAX_THREADS 1024

/**
 * Testing Harness
 */
class TestingHarness {
 public:
  TestingHarness(const TestingHarness &) = delete;
  TestingHarness &operator=(const TestingHarness &) = delete;
  TestingHarness(TestingHarness &&) = delete;
  TestingHarness &operator=(TestingHarness &&) = delete;

  // global singleton
  static TestingHarness &GetInstance(void);

  uint64_t GetThreadId();

  txn_id_t GetNextTransactionId();

  VarlenPool *GetTestingPool();

  oid_t GetNextTileGroupId();

 private:
  TestingHarness();

  // Txn id counter
  std::atomic<txn_id_t> txn_id_counter;

  // Commit id counter
  std::atomic<cid_t> cid_counter;

  // Tile group id counter
  std::atomic<oid_t> tile_group_id_counter;

  // Testing pool
  std::unique_ptr<VarlenPool> pool_;
};

template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args...));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

//===--------------------------------------------------------------------===//
// Peloton Test
//===--------------------------------------------------------------------===//

// All tests inherit from this class
class PelotonTest : public ::testing::Test {
 protected:
  virtual void SetUp() {}

  virtual void TearDown() {
    // shutdown protocol buf library
    google::protobuf::ShutdownProtobufLibrary();
  }
};

}  // End test namespace
}  // End peloton namespace
