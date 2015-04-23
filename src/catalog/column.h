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
         bool is_nullable)
 : name(name),
   type(type),
   offset(offset),
   size(size),
   is_nullable(is_nullable){
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

  bool IsNullable() const {
    return is_nullable;
  }

  std::vector<Constraint*> GetConstraints() const {
    return constraints;
  }

  bool AddConstraint(Constraint* constraint);
  Constraint* GetConstraint(const std::string &constraint_name) const;
  bool RemoveConstraint(const std::string &constraint_name);

 private:
  std::string name;

  // The data type of the column
  ValueType type = VALUE_TYPE_INVALID;

  // The column's order in the table
  oid_t offset = 0;

  // Size of the column
  size_t size = 0;

  // Is it nullable ?
  bool is_nullable = false;

  // constraints for column
  std::vector<Constraint*> constraints;
};

} // End catalog namespace
} // End nstore namespace
