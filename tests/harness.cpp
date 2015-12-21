//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// harness.cpp
//
// Identification: tests/harness.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"

#include "backend/common/pool.h"

namespace peloton {
namespace test {

/**
 * @brief Return the singleton testing harness instance
 */
TestingHarness& TestingHarness::GetInstance(){
  static TestingHarness testing_harness;
  return testing_harness;
}

TestingHarness::TestingHarness()
: txn_id_counter(INVALID_TXN_ID),
  cid_counter(INVALID_CID),
  tile_group_id_counter(START_OID),
  pool_(new VarlenPool()){
}

uint64_t TestingHarness::GetThreadId() {
  std::hash<std::thread::id> hash_fn;

  uint64_t id = hash_fn(std::this_thread::get_id());
  id = id % MAX_THREADS;

  return id;
}

VarlenPool *TestingHarness::GetTestingPool(){
  // return pool
  return pool_.get();
}

oid_t TestingHarness::GetNextTileGroupId() {
  return ++tile_group_id_counter;
}

}  // End test namespace
}  // End peloton namespace
