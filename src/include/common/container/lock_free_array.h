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
#include <array>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include "tbb/concurrent_vector.h"
#include "tbb/tbb_allocator.h"

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
  void Clear();

  // Exists ?
  bool Contains(const ValueType &value);

  /**
   * Wrapper iterator class around tbb_iterator
   */
  class Iterator {
    using tbb_iterator = tbb::internal::vector_iterator<
        tbb::concurrent_vector<ValueType, tbb::zero_allocator<ValueType>>,
        ValueType>;

   public:
    Iterator(tbb_iterator iter) : iter_(iter) {}

    inline Iterator &operator++() {
      ++iter_;
      return *this;
    }

    inline ValueType &operator*() { return *iter_; }

    inline bool operator==(const Iterator &rhs) { return iter_ == rhs.iter_; }
    inline bool operator!=(const Iterator &rhs) { return iter_ != rhs.iter_; }

   private:
    tbb_iterator iter_;
  };

  Iterator Begin() { return Iterator(lock_free_array.begin()); }

  Iterator End() { return Iterator(lock_free_array.end()); }

 private:
  // lock free array
  tbb::concurrent_vector<ValueType, tbb::zero_allocator<ValueType>>
      lock_free_array;
};

}  // namespace peloton
