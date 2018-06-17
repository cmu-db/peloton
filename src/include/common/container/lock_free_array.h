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

namespace peloton {

static const size_t kLockfreeArrayInitSize = 256;

// LOCK_FREE_ARRAY_TEMPLATE_ARGUMENTS

// LOCK_FREE_ARRAY_TYPE
#define LOCK_FREE_ARRAY_TYPE LockFreeArray<ValueType>

template <typename ValueType>
class LockFreeArray {
 public:
  LockFreeArray();
  ~LockFreeArray();

  /**
   * Assigns the provided value to the provided offset.
   *
   * @param offset  Element offset to update
   * @param value   Value to be assigned
   */
  void Update(const std::size_t &offset, const ValueType &value);

  /**
   * Appends an element to the end of the array
   *
   * @param value   Value to be appended
   */
  void Append(const ValueType &value);

  /**
   * Returns the element at the offset
   *
   * @returns   Element at offset
   */
  ValueType Find(const std::size_t &offset) const;

  /**
   * Returns the element at the offset, or invalid_value if
   * the element does not exist.
   *
   * @param offset          Element offset to access
   * @param invalid_value   Sentinel value to return if element
   *                        does not exist or offset out of range
   * @returns               Element at offset or invalid_value
   */
  ValueType FindValid(const std::size_t &offset,
                      const ValueType &invalid_value) const;

  /**
   * Assigns the provided invalid_value to the provided offset.
   *
   * @param offset          Element offset to update
   * @param invalid_value   Invalid value to be assigned
   */
  void Erase(const std::size_t &offset, const ValueType &invalid_value);

  /**
   *
   * @return Number of elements in the underlying structure
   */
  size_t GetSize() const;

  /**
   *
   * @return True if empty, false otherwise
   */
  bool IsEmpty() const;

  /**
   * Resets the underlying data structure to have 0 elements
   */
  void Clear();

  /**
   *
   * Check the lock-free array for the provided value.
   * O(n) time complexity.
   *
   * @param value  value to search for
   * @return       True if element present, false otherwise
   */
  bool Contains(const ValueType &value) const;

 private:
  // lock free array
  tbb::concurrent_vector<ValueType, tbb::zero_allocator<ValueType>>
      lock_free_array;
};

}  // namespace peloton
