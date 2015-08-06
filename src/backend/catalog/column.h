//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// column.h
//
// Identification: src/backend/catalog/column.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/catalog/constraint.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Column
//===--------------------------------------------------------------------===//

class Column {
  friend class Constraint;

 public:
  Column(){};

  Column(ValueType value_type, oid_t column_length, std::string column_name,
         bool is_inlined = false, oid_t column_offset = INVALID_OID)
      : column_type(value_type),
        column_name(column_name),
        is_inlined(is_inlined),
        column_offset(column_offset) {
    SetInlined();

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

  ValueType GetType() const { return column_type; }

  bool IsInlined() const { return is_inlined; }

  // Add a constraint to the column
  void AddConstraint(const catalog::Constraint &constraint) {
    constraints.push_back(constraint);
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
  friend std::ostream &operator<<(std::ostream &os, const Column &column);

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // value type of column
  ValueType column_type = VALUE_TYPE_INVALID;

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

  // offset of column in tuple
  oid_t column_offset = INVALID_OID;

  // Constraints
  std::vector<Constraint> constraints;
};

}  // End catalog namespace
}  // End peloton namespace
