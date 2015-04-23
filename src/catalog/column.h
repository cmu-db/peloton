/*-------------------------------------------------------------------------
 *
 * column.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/column.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "common/types.h"
#include "catalog/constraint.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Column
//===--------------------------------------------------------------------===//

class Column {

 public:
  Column(std::string name,
         ValueType type,
         oid_t offset,
         size_t size,
         bool not_null)
 : name(name),
   type(type),
   offset(offset),
   size(size),
   not_null(not_null){
  }

  std::string GetName() {
    return name;
  }

  ValueType GetType() const {
    return type;
  }

  oid_t GetOffset() const {
    return offset;
  }

  size_t GetSize() const {
    return size;
  }

  bool IsNotNullable() const {
    return not_null;
  }

 private:
  std::string name;

  // The data type of the column
  ValueType type = VALUE_TYPE_INVALID;

  // The column's order in the table
  oid_t offset = 0;

  // Size of the column
  size_t size = 0;

  // Is it not nullable ?
  bool not_null = false;

};

} // End catalog namespace
} // End nstore namespace
