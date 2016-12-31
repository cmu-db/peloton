//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen_pool.h
//
// Identification: src/backend/type/varlen_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdlib>

namespace peloton {
namespace type {

// Interface of a memory pool that can quickly allocate chunks of memory
class AbstractPool {
public:

  // Empty virtual destructor for proper cleanup
  virtual ~AbstractPool(){}

  // Allocate a contiguous block of memory of the given size. If the allocation
  // is successful a non-null pointer is returned. If the allocation fails, a
  // null pointer will be returned.
  // TODO: Provide good error codes for failure cases.
  virtual void *Allocate(size_t size) = 0;

  // Returns the provided chunk of memory back into the pool
  virtual void Free(void *ptr) = 0;

};

}  // namespace type
}  // namespace peloton
