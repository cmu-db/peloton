//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lock_free_lock_free_array.cpp
//
// Identification: src/container/lock_free_lock_free_array.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/container/lock_free_array.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/internal_types.h"

namespace peloton {

namespace index {
class Index;
}

namespace storage {
class TileGroup;
class Database;
class IndirectionArray;
}

template <typename ValueType>
LOCK_FREE_ARRAY_TYPE::LockFreeArray() {
  lock_free_array.reserve(kLockfreeArrayInitSize);
}

template <typename ValueType>
LOCK_FREE_ARRAY_TYPE::~LockFreeArray() { lock_free_array.clear(); }

template <typename ValueType>
bool LOCK_FREE_ARRAY_TYPE::Update(const std::size_t &offset, ValueType value) {
  LOG_TRACE("Update at %lu", offset);
  PELOTON_ASSERT(lock_free_array.size() >= offset + 1);
  lock_free_array.at(offset) = value;
  return true;
}

template <typename ValueType>
bool LOCK_FREE_ARRAY_TYPE::Append(ValueType value) {
  LOG_TRACE("Appended value.");
  lock_free_array.push_back(value);
  return true;
}

template <typename ValueType>
bool LOCK_FREE_ARRAY_TYPE::Erase(const std::size_t &offset,
                                 const ValueType &invalid_value) {
  LOG_TRACE("Erase at %lu", offset);
  lock_free_array.at(offset) = invalid_value;
  return true;
}

template <typename ValueType>
ValueType LOCK_FREE_ARRAY_TYPE::Find(const std::size_t &offset) const {
  LOG_TRACE("Find at %lu", offset);
  PELOTON_ASSERT(lock_free_array.size() > offset);
  auto value = lock_free_array.at(offset);
  return value;
}

template <typename ValueType>
ValueType LOCK_FREE_ARRAY_TYPE::FindValid(
    const std::size_t &offset, const ValueType &invalid_value) const {
  LOG_TRACE("Find Valid at %lu", offset);

  std::size_t valid_array_itr = 0;
  std::size_t array_itr;
  auto lock_free_array_offset = lock_free_array.size();
  for (array_itr = 0; array_itr < lock_free_array_offset; array_itr++) {
    auto value = lock_free_array.at(array_itr);
    if (value != invalid_value) {
      // Check offset
      if (valid_array_itr == offset) {
        return value;
      }

      // Update valid value count
      valid_array_itr++;
    }
  }

  return invalid_value;
}

template <typename ValueType>
size_t LOCK_FREE_ARRAY_TYPE::GetSize() const { return lock_free_array.size(); }

template <typename ValueType>
bool LOCK_FREE_ARRAY_TYPE::IsEmpty() const { return lock_free_array.empty(); }

template <typename ValueType>
void LOCK_FREE_ARRAY_TYPE::Clear() { lock_free_array.clear(); }

template <typename ValueType>
bool LOCK_FREE_ARRAY_TYPE::Contains(const ValueType &value) {
  bool exists = false;

  for (std::size_t array_itr = 0; array_itr < lock_free_array.size();
       array_itr++) {
    auto array_value = lock_free_array.at(array_itr);
    // Check array value
    if (array_value == value) {
      exists = true;
      break;
    }
  }

  return exists;
}

// Explicit template instantiation
template class LockFreeArray<std::shared_ptr<oid_t>>;

template class LockFreeArray<std::shared_ptr<index::Index>>;

template class LockFreeArray<oid_t>;

}  // namespace peloton
