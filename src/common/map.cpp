//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// map.cpp
//
// Identification: src/common/map.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <functional>

#include "common/map.h"
#include "common/logger.h"

namespace peloton {

MAP_TEMPLATE_ARGUMENTS
MAP_TYPE::Map(){
}

MAP_TEMPLATE_ARGUMENTS
MAP_TYPE::~Map(){
}

MAP_TEMPLATE_ARGUMENTS
bool MAP_TYPE::Insert(const KeyType &key, MappedType pVal){
  return avl_tree.insert(key, pVal);
}

MAP_TEMPLATE_ARGUMENTS
std::pair<bool, bool> MAP_TYPE::Update(const KeyType &key,
                                       MappedType pVal,
                                       bool bInsert){
  return avl_tree.update(key, pVal, bInsert);
}

MAP_TEMPLATE_ARGUMENTS
bool MAP_TYPE::Erase(const KeyType &key){
  return avl_tree.erase(key);
}

MAP_TEMPLATE_ARGUMENTS
bool MAP_TYPE::Find(const KeyType &key, ValueType& value){

  auto status = avl_tree.find(key, [&value](const KeyType&,
      FuncMappedType& map_value){
    value = map_value;
  });

  return status;
}

MAP_TEMPLATE_ARGUMENTS
bool MAP_TYPE::Contains(const KeyType &key){
  return avl_tree.contains(key);
}

MAP_TEMPLATE_ARGUMENTS
void MAP_TYPE::Clear(){
  avl_tree.clear();
}

MAP_TEMPLATE_ARGUMENTS
size_t MAP_TYPE::GetSize() const{
  return avl_tree.size();
}

MAP_TEMPLATE_ARGUMENTS
bool MAP_TYPE::IsEmpty() const{
  return avl_tree.empty();
}

MAP_TEMPLATE_ARGUMENTS
bool MAP_TYPE::CheckConsistency() const{
  return avl_tree.check_consistency();
}

// Explicit template instantiation
template class Map<uint32_t, uint32_t>;

}  // End peloton namespace
