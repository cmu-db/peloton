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

std::atomic<txn_id_t> txn_id_counter(INVALID_TXN_ID);
std::atomic<cid_t> cid_counter(INVALID_CID);
std::atomic<oid_t> tile_group_id_counter(START_OID);

uint64_t GetThreadId() {
  std::hash<std::thread::id> hash_fn;

  uint64_t id = hash_fn(std::this_thread::get_id());
  id = id % MAX_THREADS;

  return id;
}

static std::unique_ptr<VarlenPool> pool_;

VarlenPool *GetTestingPool(){
  // construct pool if needed
  if(pool_.get() == nullptr)
    pool_.reset(new VarlenPool());

  // return pool
  return pool_.get();
}

}  // End test namespace
}  // End peloton namespace
