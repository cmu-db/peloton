//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ephemeral_pool.h
//
// Identification: src/include/type/ephemeral_pool.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <cstdlib>
#include <unordered_set>

#include "common/macros.h"
#include "common/synchronization/spin_latch.h"
#include "type/abstract_pool.h"

namespace peloton {
namespace type {

//===----------------------------------------------------------------------===//
//
// A memory pool that can quickly allocate chunks of memory to clients.
//
//===----------------------------------------------------------------------===//
class EphemeralPool : public AbstractPool {
 public:
  EphemeralPool() = default;

  ~EphemeralPool();

  void *Allocate(size_t size) override;

  void Free(void *ptr) override;

 public:
  // Location list
  std::unordered_set<char *> locations_;

  // Spin lock protecting location list
  common::synchronization::SpinLatch pool_lock_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline EphemeralPool::~EphemeralPool() {
  pool_lock_.Lock();
  for (auto location : locations_) {
    delete[] location;
  }
  pool_lock_.Unlock();
}

inline void *EphemeralPool::Allocate(size_t size) {
  auto location = new char[size];

  pool_lock_.Lock();
  locations_.insert(location);
  pool_lock_.Unlock();

  return location;
}

inline void EphemeralPool::Free(void *ptr) {
  auto *cptr = (char *)ptr;
  pool_lock_.Lock();
  locations_.erase(cptr);
  pool_lock_.Unlock();
  delete[] cptr;
}

}  // namespace type
}  // namespace peloton
