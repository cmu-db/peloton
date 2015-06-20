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

#include "backend/common/types.h"

#include <mutex>
#include <vector>
#include <iostream>

namespace nstore {

namespace index{
class Index;
}

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

  ~Index() {
    // don't clean up columns here
    // they will cleaned in their respective tables
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

  void SetPhysicalIndex(index::Index* index) {
    physical_index = index;
  }

  index::Index *GetPhysicalIndex() {
    return physical_index;
  }

  // Get a string representation of this index
  friend std::ostream& operator<<(std::ostream& os, const Index& index);

  void Lock(){
    index_mtx.lock();
  }

  void Unlock(){
    index_mtx.unlock();
  }

 private:
  std::string name;

  // What data structure is the index using ?
  IndexType type = INDEX_TYPE_INVALID;

  // Can the index contain duplicate keys?
  bool unique = false;

  // Columns referenced by the index
  std::vector<Column*> columns;

  // underlying physical index
  index::Index* physical_index = nullptr;

  std::mutex index_mtx;
};

} // End catalog namespace
} // End nstore namespace
