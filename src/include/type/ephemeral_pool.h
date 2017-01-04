//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ephemeral_pool.h
//
// Identification: src/backend/type/ephemeral_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"
#include "type/abstract_pool.h"

#include <stdint.h>
#include <stdlib.h>
#include <vector>

#include "common/platform.h"

namespace peloton {
namespace type {

// A memory pool that can quickly allocate chunks of memory to clients.
class EphemeralPool : public AbstractPool {
public:

  EphemeralPool(){

  }

  // Destroy this pool, and all memory it owns.
  ~EphemeralPool(){

    pool_lock_.Lock();
    for(auto location: locations_){
      delete[] location;
    }
    pool_lock_.Unlock();

  }

  // Allocate a contiguous block of memory of the given size. If the allocation
  // is successful a non-null pointer is returned. If the allocation fails, a
  // null pointer will be returned.
  void *Allocate(size_t size){
    auto location = new char[size];

    pool_lock_.Lock();
    locations_.push_back(location);
    pool_lock_.Unlock();

    return location;
  }

  // Returns the provided chunk of memory back into the pool
  void Free(UNUSED_ATTRIBUTE void *ptr){

  }

public:

  // Location list
  std::vector<char*> locations_;

  // Spin lock protecting location list
  Spinlock pool_lock_;

};

}  // namespace type
}  // namespace peloton
