//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skip_list_map.cpp
//
// Identification: src/container/skip_list_map.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <functional>
#include <iostream>
#include <mutex>

#include "container/skip_list_map.h"

#include "common/logger.h"
#include "common/macros.h"
#include "common/types.h"

#include "index/index_key.h"

namespace peloton {

SKIP_LIST_MAP_TEMPLATE_ARGUMENTS
SKIP_LIST_MAP_TYPE::SkipListMap(){

  LOG_TRACE("Creating Skip List Map");

}

SKIP_LIST_MAP_TEMPLATE_ARGUMENTS
SKIP_LIST_MAP_TYPE::~SkipListMap(){

  LOG_TRACE("Destroying Skip List Map");

}

SKIP_LIST_MAP_TEMPLATE_ARGUMENTS
bool SKIP_LIST_MAP_TYPE::Insert(const KeyType &key, ValueType value){
  auto iterator = skip_list_map.insert(key, value);
  auto status = (iterator != skip_list_map.end());
  LOG_TRACE("insert status : %d", status);
  return status;
}

SKIP_LIST_MAP_TEMPLATE_ARGUMENTS
bool SKIP_LIST_MAP_TYPE::Update(const KeyType &key, ValueType value, const bool bInsert){
  auto status =  skip_list_map.update(key, bInsert);
  auto iterator = status.first;

  // found
  if(iterator != skip_list_map.end()){
    iterator->second = value;
  }

  return status.second;
}

SKIP_LIST_MAP_TEMPLATE_ARGUMENTS
bool SKIP_LIST_MAP_TYPE::Erase(const KeyType &){
  const bool status = false;
  LOG_TRACE("erase status : %d", status);
  return status;
}

SKIP_LIST_MAP_TEMPLATE_ARGUMENTS
bool SKIP_LIST_MAP_TYPE::Find(const KeyType &key,
                              ValueType& value){
  auto iterator = skip_list_map.contains(key);
  auto status = (iterator != skip_list_map.end());

  // found
  if(status == true){
    value = iterator->second;
  }

  LOG_TRACE("find status : %d", status);
  return status;
}

SKIP_LIST_MAP_TEMPLATE_ARGUMENTS
void SKIP_LIST_MAP_TYPE::Clear(){
  skip_list_map.clear();
}

SKIP_LIST_MAP_TEMPLATE_ARGUMENTS
size_t SKIP_LIST_MAP_TYPE::GetSize() const{
  return skip_list_map.size();
}

SKIP_LIST_MAP_TEMPLATE_ARGUMENTS
bool SKIP_LIST_MAP_TYPE::IsEmpty() const{
  return skip_list_map.empty();
}

// Explicit template instantiation

template class SkipListMap<index::GenericKey<4>, ItemPointer *, index::GenericComparatorRaw<4>>;
template class SkipListMap<index::GenericKey<8>, ItemPointer *, index::GenericComparatorRaw<8>>;
template class SkipListMap<index::GenericKey<16>, ItemPointer *, index::GenericComparatorRaw<16>>;
template class SkipListMap<index::GenericKey<64>, ItemPointer *, index::GenericComparatorRaw<64>>;
template class SkipListMap<index::GenericKey<256>, ItemPointer *, index::GenericComparatorRaw<256>>;

template class SkipListMap<index::TupleKey, ItemPointer *, index::TupleKeyComparatorRaw>;

}  // End peloton namespace
