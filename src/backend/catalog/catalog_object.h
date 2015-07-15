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
#include <vector>
#include <map>

#include "backend/common/types.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
//  Catalog Object
//===--------------------------------------------------------------------===//

/**
 * Base type of all catalog objects
 * Each object has a set of collections.
 * Each collection has a set of children.
 * For instance, each DB has a list of tables.
 */
class CatalogObject {

 public:

  // Constructor
  CatalogObject(oid_t oid = INVALID_OID,
                CatalogObject *parent = nullptr,
                CatalogObject *root = nullptr) :
    oid_(oid),
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

  /**
   * @brief Adds a child to this catalog object.
   * @param collection_offset the offset of the collection
   * @param child pointer to the child itself, which must also be a CatalogObject.
   */
  void AddChild(const oid_t collection_offset, CatalogObject *child);

  /**
   * @brief Get child from this catalog object based on its offset
   * @param collection_offset the offset of the collection
   * @param child_offset the offset of the child
   * @return the child if successful, nullptr otherwise
   */
  CatalogObject *GetChild(const oid_t collection_offset,
                          const oid_t child_offset) const;

  /**
   * @brief Get child from this catalog object based on its oid
   * @param collection_offset the offset of the collection
   * @param child_id the identifier for the child that is unique atleast within this object.
   * @return the child if successful, nullptr otherwise
   */
  CatalogObject *GetChildWithID(const oid_t collection_offset,
                                const oid_t child_id) const;

  /**
   * @brief Drop the child at given offset from this catalog object.
   * @param collection_offset the offset of the collection
   * @param child_offset the offset of the child
   */
  void DropChild(const oid_t collection_offset,
                 const oid_t child_offset);

  /**
   * @brief Drop the child with given id from this catalog object.
   * @param collection_offset the offset of the collection
   * @param child_id the id of the child
   */
  void DropChildWithID(const oid_t collection_offset,
                       const oid_t child_id);

  /**
   * @brief Get the count of children.
   * @param collection_offset the offset of the collection
   */
  size_t GetChildrenCount(const oid_t collection_offset) const;

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
  oid_t oid_;

  // parent of this CatalogType instance
  CatalogObject *parent_;

  // children list
  std::map<oid_t, std::vector<CatalogObject*> > children_;

  // pointer to the root Catalog instance
  CatalogObject *root_;

};

} // End catalog namespace
} // End peloton namespace


