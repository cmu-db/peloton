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

#include "common/container/lock_free_array.h"
#include "common/internal_types.h"
#include "common/logger.h"
#include "common/macros.h"

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
void LOCK_FREE_ARRAY_TYPE::Update(const std::size_t &offset,
                                  const ValueType &value) {
  LOG_TRACE("Update at %lu", offset);
  PELOTON_ASSERT(lock_free_array.size() > offset);
  lock_free_array.at(offset) = value;
}

template <typename ValueType>
void LOCK_FREE_ARRAY_TYPE::Append(const ValueType &value) {
  LOG_TRACE("Appended value.");
  lock_free_array.push_back(value);
}

template <typename ValueType>
void LOCK_FREE_ARRAY_TYPE::Erase(const std::size_t &offset,
                                 const ValueType &invalid_value) {
  LOG_TRACE("Erase at %lu", offset);
  lock_free_array.at(offset) = invalid_value;
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

  ValueType value = invalid_value;
  if ((lock_free_array.size() > offset)) {
    value = lock_free_array.at(offset);
  }

  return value;
}

template <typename ValueType>
size_t LOCK_FREE_ARRAY_TYPE::GetSize() const { return lock_free_array.size(); }

template <typename ValueType>
bool LOCK_FREE_ARRAY_TYPE::IsEmpty() const { return lock_free_array.empty(); }

template <typename ValueType>
void LOCK_FREE_ARRAY_TYPE::Clear() {
  // Intel docs: To free internal arrays, call shrink_to_fit() after clear().
  lock_free_array.clear();
  lock_free_array.shrink_to_fit();
}

template <typename ValueType>
bool LOCK_FREE_ARRAY_TYPE::Contains(const ValueType &value) const {
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
