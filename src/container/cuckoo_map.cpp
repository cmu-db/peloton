//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cuckoo_map.cpp
//
// Identification: src/container/cuckoo_map.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <functional>
#include <iostream>

#include "container/cuckoo_map.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/types.h"

#include "libcds/cds/init.h"

namespace peloton {

namespace storage{
class TileGroup;
}


CUCKOO_MAP_TEMPLATE_ARGUMENTS
CUCKOO_MAP_TYPE::CuckooMap(){
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
CUCKOO_MAP_TYPE::~CuckooMap(){
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Insert(const KeyType &key, ValueType value){
  auto status = cuckoo_map.insert(key, value);
  LOG_INFO("insert status : %d", status);
  return status;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
std::pair<bool, bool> CUCKOO_MAP_TYPE::Update(const KeyType &key,
                                              ValueType value,
                                              bool bInsert){

  auto status =  cuckoo_map.update(key, [&value]( bool bNew, map_pair& found_pair) {
    if(bNew == true) {
      // new entry
      LOG_INFO("New entry");
    }
    else {
      // found existing entry
      found_pair.second = value;
      LOG_INFO("Updating existing entry");
    }
  }, bInsert);

  return status;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Erase(const KeyType &key){

  auto status = cuckoo_map.erase(key);
  LOG_INFO("erase status : %d", status);

  return status;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Find(const KeyType &key,
                           ValueType& value){

  auto status = cuckoo_map.find(key, [&value]( map_pair& found_value) {
    value = found_value.second;
  } );
  LOG_INFO("find status : %d", status);
  return status;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Contains(const KeyType &key){
  return cuckoo_map.contains(key);
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
void CUCKOO_MAP_TYPE::Clear(){
  cuckoo_map.clear();
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
size_t CUCKOO_MAP_TYPE::GetSize() const{
  return cuckoo_map.size();
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::IsEmpty() const{
  return cuckoo_map.empty();
}

// Explicit template instantiation
template class CuckooMap<uint32_t, uint32_t>;

template class CuckooMap<oid_t, std::shared_ptr<storage::TileGroup>>;

template class CuckooMap<oid_t, std::shared_ptr<oid_t>>;

}  // End peloton namespace
