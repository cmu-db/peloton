//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ephemeral_pool.h
//
// Identification: src/include/type/ephemeral_pool.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <vector>
#include <unordered_map>

#include "common/macros.h"
#include "common/synchronization/spin_latch.h"
#include "type/abstract_pool.h"

namespace peloton {
namespace type {

// A memory pool that can quickly allocate chunks of memory to clients.
class EphemeralPool : public AbstractPool {
public:

  EphemeralPool() : mem_comsume_{0} {}

  // Destroy this pool, and all memory it owns.
  ~EphemeralPool(){

    pool_lock_.Lock();
    for(auto &entry: locations_){
      delete[] entry.first;
    }
    pool_lock_.Unlock();

  }

  // Allocate a contiguous block of memory of the given size. If the allocation
  // is successful a non-null pointer is returned. If the allocation fails, a
  // null pointer will be returned.
  void *Allocate(size_t size){
    auto location = new char[size];

    pool_lock_.Lock();
    locations_[location] = size;
    mem_comsume_ += size;
    pool_lock_.Unlock();

    return location;
  }

  // Returns the provided chunk of memory back into the pool
  void Free(UNUSED_ATTRIBUTE void *ptr) {
    char *cptr = (char *) ptr;
    pool_lock_.Lock();
    size_t block_size = locations_[cptr];
    mem_comsume_ -= block_size;
    locations_.erase(cptr);
    pool_lock_.Unlock();
    delete [] cptr;
  }

  /**
   * @see AbstractPool
   */
  inline size_t GetMemoryAlloc () override {
    return mem_comsume_.load();
  };

  /**
   * @see AbstractPool
   */
  inline size_t GetMemoryUsage () override {
      return mem_comsume_.load();
  };

public:

  // Location list
  std::unordered_map<char*, size_t> locations_;

  // Spin lock protecting location list
  common::synchronization::SpinLatch pool_lock_;

  /**
   * Memory usage as well as allocation
   */
  std::atomic<size_t> mem_comsume_;
};

}  // namespace type
}  // namespace peloton
