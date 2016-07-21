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

#include "container/lock_free_array.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/types.h"

namespace peloton {

namespace index{
class Index;
}

LOCK_FREE_ARRAY_TEMPLATE_ARGUMENTS
LOCK_FREE_ARRAY_TYPE::LockFreeArray(){
  lock_free_array.reset(new lock_free_array_t());
}

LOCK_FREE_ARRAY_TEMPLATE_ARGUMENTS
LOCK_FREE_ARRAY_TYPE::~LockFreeArray(){
}

LOCK_FREE_ARRAY_TEMPLATE_ARGUMENTS
bool LOCK_FREE_ARRAY_TYPE::Update(const std::size_t &offset, ValueType value){
  PL_ASSERT(offset <= lock_free_array_offset);
  LOG_TRACE("Update at %lu", lock_free_array_offset.load());
  lock_free_array->at(offset) =  value;
  return true;
}

LOCK_FREE_ARRAY_TEMPLATE_ARGUMENTS
bool LOCK_FREE_ARRAY_TYPE::Append(ValueType value){
  LOG_TRACE("Appended at %lu", lock_free_array_offset.load());
  lock_free_array->at(lock_free_array_offset++) = value;
  return true;
}

LOCK_FREE_ARRAY_TEMPLATE_ARGUMENTS
bool LOCK_FREE_ARRAY_TYPE::Erase(const std::size_t &offset, ValueType& value){
  PL_ASSERT(offset <= lock_free_array_offset);
  LOG_TRACE("Erase at %lu", offset);
  lock_free_array->at(offset) =  value;
  return true;
}

LOCK_FREE_ARRAY_TEMPLATE_ARGUMENTS
bool LOCK_FREE_ARRAY_TYPE::Find(const std::size_t &offset, ValueType& value){
  PL_ASSERT(offset <= lock_free_array_offset);
  LOG_TRACE("Find at %lu", offset);
  value = lock_free_array->at(offset);
  return true;
}

LOCK_FREE_ARRAY_TEMPLATE_ARGUMENTS
size_t LOCK_FREE_ARRAY_TYPE::GetSize() const{
  return lock_free_array_offset;
}

LOCK_FREE_ARRAY_TEMPLATE_ARGUMENTS
bool LOCK_FREE_ARRAY_TYPE::IsEmpty() const{
  return lock_free_array->empty();
}

// Explicit template instantiation
template class LockFreeArray<uint32_t>;

template class LockFreeArray<std::shared_ptr<oid_t>>;

template class LockFreeArray<std::shared_ptr<index::Index>>;

}  // End peloton namespace
