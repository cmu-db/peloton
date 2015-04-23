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

#include "common/types.h"

namespace nstore {
namespace catalog {

class Column;

//===--------------------------------------------------------------------===//
// Index
//===--------------------------------------------------------------------===//

class Index {

 public:
  Index(std::string name,
        IndexType type,
        bool unique,
        std::vector<Column*> columns)
 : name(name),
   type(type),
   unique(unique),
   columns(columns){
  }

  std::string GetName() {
    return name;
  }

  IndexType GetType() const {
    return type;
  }

  bool IsUnique() const {
    return unique;
  }

  std::vector<Column*> GetColumns() const {
    return columns;
  }

 private:
  std::string name;

  // What data structure is the index using ?
  IndexType type = INDEX_TYPE_INVALID;

  // Can the index contain duplicate keys?
  bool unique = false;

  // Columns referenced by the index
  std::vector<Column*> columns;
};

} // End catalog namespace
} // End nstore namespace
