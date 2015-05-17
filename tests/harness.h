/*-------------------------------------------------------------------------
 *
 * multithreaded_tester.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/test/multithreaded_tester.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <vector>
#include <thread>
#include <functional>
#include <iostream>

#include "common/types.h"
#include "common/pretty_printer.h"
#include "common/logger.h"
#include "common/context.h"

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Test Harness (common routines)
//===--------------------------------------------------------------------===//

#define MAX_THREADS 1024

extern std::atomic<txn_id_t> txn_id_counter;
extern std::atomic<cid_t> cid_counter;
extern std::atomic<oid_t> tile_group_id_counter;

uint64_t GetThreadId();

template<typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args&&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args...));
  }

  //Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

inline txn_id_t GetNextTransactionId() {
  return ++txn_id_counter;
}

inline cid_t GetNextCommitId() {
  return ++cid_counter;
}

inline oid_t GetNextTileGroupId(){
  return ++tile_group_id_counter;
}

inline Context GetNextContext(){
  const txn_id_t txn_id = GetNextTransactionId();
  const cid_t commit_id = GetNextCommitId();
  Context context(txn_id, commit_id);
  return context;
}


} // End test namespace
} // End nstore namespace



