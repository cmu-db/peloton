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

#include "backend/catalog/catalog_object.h"

namespace peloton {
namespace catalog {

CatalogObject::~CatalogObject() {

  // Clean up all the children
  for(auto child : children_)
    delete child;

}


bool CatalogObject::CreateChild(CatalogObject *child){

  // Access the children map safely
  {
    std::lock_guard<std::mutex> lock(mutex_);
    children_.push_back(child);
  }

  return true;
}

CatalogObject *CatalogObject::GetChild(const oid_t child_offset) const {

  // Access the children map safely
  {
    std::lock_guard<std::mutex> lock(mutex_);

    if(child_offset >= children_.size())
      return nullptr;

    return children_[child_offset];
  }

}

CatalogObject *CatalogObject::GetChildWithOid(const oid_t child_id) const {

  // Access the children map safely
  {
    std::lock_guard<std::mutex> lock(mutex_);

    // Go over all children checking their oid
    for(auto child : children_) {
      if(child->oid_ == child_id)
        return child;
    }

    return nullptr;
  }

}

bool CatalogObject::DropChild(const oid_t child_id) {

  // Access the children map safely
  {
    std::lock_guard<std::mutex> lock(mutex_);

    if(child_id >= children_.size())
      return false;

    children_.erase(children_.begin() + child_id);
  }

  return true;
};

} // End catalog namespace
} // End peloton namespace



