//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// constraint.h
//
// Identification: src/backend/catalog/constraint.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "backend/common/types.h"

#include "nodes/nodes.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Constraint Class
//===--------------------------------------------------------------------===//

class Constraint {
 public:
  Constraint(ConstraintType type,
             std::string constraint_name,
             Node *raw_expr = nullptr)
      : constraint_type(type),
        constraint_name(constraint_name),
        expr(raw_expr) {
  }

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  ConstraintType GetType() const { return constraint_type; }

  // Offset into the list of "reference tables" in the Table.
  void SetForeignKeyListOffset(oid_t offset) { fk_list_offset = offset; }

  // Offset into the list of "unique indices" in the Table.
  void SetUniqueIndexOffset(oid_t offset) { unique_index_list_offset = offset; }

  // Get the offset
  oid_t GetForeignKeyListOffset() const { return fk_list_offset; }

  // Get the offset
  oid_t GetUniqueIndexOffset() const { return unique_index_list_offset; }

  std::string GetName() const { return constraint_name; }

  // Get a string representation of this constraint
  friend std::ostream &operator<<(std::ostream &os,
                                  const Constraint &constraint);

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The type of constraint
  ConstraintType constraint_type = CONSTRAINT_TYPE_INVALID;

  // Offsets into the Unique index and reference table lists in Table
  oid_t fk_list_offset = INVALID_OID;

  oid_t unique_index_list_offset = INVALID_OID;

  std::string constraint_name;

  // Default_expr
  Node *expr = nullptr;
};

}  // End catalog namespace
}  // End peloton namespace
