/*-------------------------------------------------------------------------
 *
 * index.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/index.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <mutex>
#include <vector>
#include <iostream>

#include "backend/common/types.h"
#include "backend/catalog/catalog_object.h"

namespace peloton {

namespace index{
class Index;
}

namespace catalog {

class Column;

//===--------------------------------------------------------------------===//
// Index
//===--------------------------------------------------------------------===//

class Index : public CatalogObject {

 public:

  Index(oid_t index_oid,
        std::string index_name,
        CatalogObject *parent,
        CatalogObject *root)
 : CatalogObject(index_oid,
                 parent,
                 root),
                 index_name(index_name){
  }

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  void SetPhysicalIndex(index::Index* index) {
    physical_index = index;
  }

  index::Index *GetPhysicalIndex() const {
    return physical_index;
  }

  std::string GetName() const {
    return index_name;
  }

  // Get a string representation of this index
  friend std::ostream& operator<<(std::ostream& os, const Index& index);

 private:

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // underlying physical index
  index::Index* physical_index = nullptr;

  std::string index_name;

};

} // End catalog namespace
} // End peloton namespace
