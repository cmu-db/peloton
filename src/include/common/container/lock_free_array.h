//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lock_free_array.h
//
// Identification: src/include/container/lock_free_array.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "tbb/concurrent_vector.h"
#include "tbb/tbb_allocator.h"
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <array>
#include <atomic>
#include <memory>

namespace peloton {

#define LOCK_FREE_ARRAY_GROW_SPEED (100)

// LOCK_FREE_ARRAY_TEMPLATE_ARGUMENTS
#define LOCK_FREE_ARRAY_TEMPLATE_ARGUMENTS template <typename ValueType>

// LOCK_FREE_ARRAY_TYPE
#define LOCK_FREE_ARRAY_TYPE LockFreeArray<ValueType>

LOCK_FREE_ARRAY_TEMPLATE_ARGUMENTS
class LockFreeArray {
 public:
  LockFreeArray();
  ~LockFreeArray();

  // Update a item
  bool Update(const std::size_t &offset, ValueType value);

  // Append an item
  bool Append(ValueType value);

  // Get a item
  ValueType Find(const std::size_t &offset) const;

  // Get a valid item
  ValueType FindValid(const std::size_t &offset,
                      const ValueType &invalid_value) const;

  // Delete key from the lock_free_array
  bool Erase(const std::size_t &offset, const ValueType &invalid_value);

  // Returns item count in the lock_free_array
  size_t GetSize() const;

  // Checks if the lock_free_array is empty
  bool IsEmpty() const;

  // Clear all elements and reset them to default value
  void Clear(const ValueType &invalid_value);

  // Exists ?
  bool Contains(const ValueType &value);

 private:
  std::atomic<std::size_t> lock_free_array_offset{0};

  // lock free array
  tbb::concurrent_vector<ValueType, tbb::zero_allocator<ValueType>>
      lock_free_array;
};

}  // namespace peloton
