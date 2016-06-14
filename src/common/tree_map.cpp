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

#include "../include/common/tree_map.h"

#include <functional>

#include "common/logger.h"

namespace peloton {

TREE_MAP_TEMPLATE_ARGUMENTS
TREE_MAP_TYPE::TreeMap(){
}

TREE_MAP_TEMPLATE_ARGUMENTS
TREE_MAP_TYPE::~TreeMap(){
}

TREE_MAP_TEMPLATE_ARGUMENTS
bool TREE_MAP_TYPE::Insert(const KeyType &key, MappedType pVal){
  return avl_tree.insert(key, pVal);
}

TREE_MAP_TEMPLATE_ARGUMENTS
std::pair<bool, bool> TREE_MAP_TYPE::Update(const KeyType &key,
                                       MappedType pVal,
                                       bool bInsert){
  return avl_tree.update(key, pVal, bInsert);
}

TREE_MAP_TEMPLATE_ARGUMENTS
bool TREE_MAP_TYPE::Erase(const KeyType &key){
  return avl_tree.erase(key);
}

TREE_MAP_TEMPLATE_ARGUMENTS
bool TREE_MAP_TYPE::Find(const KeyType &key, ValueType& value){

  auto status = avl_tree.find(key, [&value](const KeyType&,
      FuncMappedType& map_value){
    value = map_value;
  });

  return status;
}

TREE_MAP_TEMPLATE_ARGUMENTS
bool TREE_MAP_TYPE::Contains(const KeyType &key){
  return avl_tree.contains(key);
}

TREE_MAP_TEMPLATE_ARGUMENTS
void TREE_MAP_TYPE::Clear(){
  avl_tree.clear();
}

TREE_MAP_TEMPLATE_ARGUMENTS
size_t TREE_MAP_TYPE::GetSize() const{
  return avl_tree.size();
}

TREE_MAP_TEMPLATE_ARGUMENTS
bool TREE_MAP_TYPE::IsEmpty() const{
  return avl_tree.empty();
}

TREE_MAP_TEMPLATE_ARGUMENTS
bool TREE_MAP_TYPE::CheckConsistency() const{
  return avl_tree.check_consistency();
}

// Explicit template instantiation
template class TreeMap<uint32_t, uint32_t>;

}  // End peloton namespace
