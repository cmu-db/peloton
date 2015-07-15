/*-------------------------------------------------------------------------
 *
 * catalog_object.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/catalog/catalog_object.cpp
 *
 *-------------------------------------------------------------------------
 */


#include <mutex>
#include <cassert>

#include "backend/catalog/catalog_object.h"

namespace peloton {
namespace catalog {

CatalogObject::~CatalogObject() {

  // Clean up all the collections
  for(auto collection : children_) {
    // Clean up all the children
    for(auto child : collection.second) {
      delete child;
    }
  }

}

void CatalogObject::AddChild(const oid_t collection_offset,
                                CatalogObject *child){
  assert(collection_offset < children_.size());

  // Add the child in the collection
  children_[collection_offset].push_back(child);
}

CatalogObject *CatalogObject::GetChild(const oid_t collection_offset,
                                       const oid_t child_offset) const {
  assert(collection_offset < children_.size());

  // Get the collection
  auto& collection = children_.at(collection_offset);
  assert(child_offset < collection.size());

  // Get the child
  return collection[child_offset];
}

CatalogObject *CatalogObject::GetChildWithID(const oid_t collection_offset,
                                              const oid_t child_id) const {
  assert(collection_offset < children_.size());

  // Get the collection
  auto& collection = children_.at(collection_offset);

  // Get the child
  for(auto child : collection) {
    if(child->oid_ == child_id)
      return child;
  }

  return nullptr;
}

void CatalogObject::DropChild(const oid_t collection_offset,
                              const oid_t child_offset) {
  assert(collection_offset < children_.size());

  // Get the collection
  auto collection = children_.at(collection_offset);
  assert(child_offset < collection.size());

  // Drop the child
  collection.erase(collection.begin() + child_offset);
};

void CatalogObject::DropChildWithID(const oid_t collection_offset,
                                     const oid_t child_id) {
  assert(collection_offset < children_.size());

  // Get the collection
  auto collection = children_.at(collection_offset);

  // Get the child offset
  oid_t child_offset = 0;
  for(auto child : collection) {
    if(child->oid_ == child_id)
      break;
    child_offset++;
  }
  assert(child_offset < collection.size());

  // Drop the child
  collection.erase(collection.begin() + child_offset);
};

size_t CatalogObject::GetChildrenCount(const oid_t collection_offset) const {
  assert(collection_offset < children_.size());

  // Get the collection
  auto& collection = children_.at(collection_offset);
  return collection.size();
}

} // End catalog namespace
} // End peloton namespace



