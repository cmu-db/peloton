//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column.h
//
// Identification: src/include/catalog/column.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/printable.h"
#include "catalog/constraint.h"
#include "common/type.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Column
//===--------------------------------------------------------------------===//

class Column : public Printable {
  friend class Constraint;

 public:
  Column() {};

  Column(common::Type::TypeId value_type, oid_t column_length,
         std::string column_name, bool is_inlined = false,
         oid_t column_offset = INVALID_OID)
      : column_type(value_type),
        column_name(column_name),
        is_inlined(is_inlined),
        column_offset(column_offset) {
    SetInlined();

    // We should not have an inline value of length 0
    if (is_inlined && column_length == 0) {
      assert(false);
    }

    SetLength(column_length);
  }

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // Set inlined
  void SetInlined();

  // Set the appropriate column length
  void SetLength(oid_t column_length);

  oid_t GetOffset() const { return column_offset; }

  std::string GetName() const { return column_name; }

  oid_t GetLength() const {
    if (is_inlined)
      return fixed_length;
    else
      return variable_length;
  }

  oid_t GetFixedLength() const { return fixed_length; }

  oid_t GetVariableLength() const { return variable_length; }

  common::Type::TypeId GetType() const { return column_type; }

  bool IsInlined() const { return is_inlined; }

  bool IsPrimary() const { return is_primary_; }

  // Add a constraint to the column
  void AddConstraint(const catalog::Constraint &constraint) {
    constraints.push_back(constraint);
    if (constraint.GetType() == CONSTRAINT_TYPE_PRIMARY) {
      is_primary_ = true;
    }
  }

  const std::vector<Constraint> &GetConstraints() const { return constraints; }

  // Compare two column objects
  bool operator==(const Column &other) const {
    if (other.column_type != column_type || other.is_inlined != is_inlined) {
      return false;
    }
    return true;
  }

  bool operator!=(const Column &other) const { return !(*this == other); }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // value type of column
  common::Type::TypeId column_type = common::Type::INVALID;

  // if the column is not inlined, this is set to pointer size
  // else, it is set to length of the fixed length column
  oid_t fixed_length = INVALID_OID;

  // if the column is inlined, this is set to 0
  // else, it is set to length of the variable length column
  oid_t variable_length = INVALID_OID;

  // name of the column
  std::string column_name;

  // is the column inlined ?
  bool is_inlined = false;

  // is the column contained the primary key?
  bool is_primary_ = false;

  // offset of column in tuple
  oid_t column_offset = INVALID_OID;

  // Constraints
  std::vector<Constraint> constraints;
};

}  // End catalog namespace
}  // End peloton namespace
