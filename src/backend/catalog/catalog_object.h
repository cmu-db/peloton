/*-------------------------------------------------------------------------
 *
 * abstract_catalog_object.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <string>
#include <mutex>
#include <vector>

#include "backend/common/types.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
//  Catalog Object
//===--------------------------------------------------------------------===//

/**
 * Base type of all catalog objects
 */
class CatalogObject {

 public:
  CatalogObject(const CatalogObject &) = delete;
  CatalogObject& operator=(const CatalogObject &) = delete;
  CatalogObject(CatalogObject &&) = delete;
  CatalogObject& operator=(CatalogObject &&) = delete;

  // Constructor
  CatalogObject(oid_t oid,
                std::string name,
                CatalogObject *parent,
                CatalogObject *root) :
    oid_(oid),
    name_(name),
    parent_(parent),
    root_(root){

    // Nothing more to do here

  }

  // Destructor
  ~CatalogObject();

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  oid_t GetOid() const {
    return oid_;
  }

  std::string GetName() const {
    return name_;
  }

  /**
   * @brief Adds a child to this catalog object.
   * @param child pointer to the child itself, which must also be a CatalogObject.
   * @return true if successful, false otherwise
   */
  bool CreateChild(CatalogObject *child);

  /**
   * @brief Get child from this catalog object based on its offset
   * @param child_offset the offset of the child
   * @return the child if successful, nullptr otherwise
   */
  CatalogObject *GetChild(const oid_t child_offset) const;

  /**
   * @brief Get child from this catalog object based on its oid
   * @param child_ the identifier for the child that is unique atleast within this object.
   * @return the child if successful, nullptr otherwise
   */
  CatalogObject *GetChildWithOid(const oid_t child_id) const;

  /**
   * @brief Drop the child from this catalog object.
   * @param child_id the offset of the child
   * @return true if successful, false otherwise
   */
  bool DropChild(const oid_t child_id);

  // Get the parent
  CatalogObject *GetParent() const {
    return parent_;
  }

  // Get the root node of this catalog instance
  CatalogObject *GetRoot() const {
    return root_;
  }

 private:

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//
  // unique within a database
  oid_t oid_ = INVALID_OID;

  // name of this catalog object
  std::string name_;

  // parent of this CatalogType instance
  CatalogObject *parent_;

  // children
  std::vector<CatalogObject*> children_;

  // pointer to the root Catalog instance
  CatalogObject *root_;

  // Synch accesses to the children map
  std::mutex mutex_;
};

} // End catalog namespace
} // End peloton namespace


