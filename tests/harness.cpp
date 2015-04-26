/**
 * @brief Utility functions for tests.
 *
 * Copyright(c) 2015, CMU
 */

#include "harness.h"

namespace nstore {
namespace test {

std::atomic<txn_id_t> txn_id_counter(INVALID_TXN_ID);
std::atomic<cid_t> cid_counter(INVALID_CID);

uint64_t GetThreadId() {
  std::hash<std::thread::id> hash_fn;

  uint64_t id = hash_fn(std::this_thread::get_id());
  id = id % MAX_THREADS;

  return id;
}

} // End test namespace
} // End nstore namespace
