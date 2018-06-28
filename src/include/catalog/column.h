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

#include "catalog/constraint.h"
#include "common/macros.h"
#include "common/printable.h"
#include "type/type.h"
#include "util/hash_util.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Column
//===--------------------------------------------------------------------===//

class Column : public Printable {
  friend class Constraint;

 public:
  Column() : column_type_(type::TypeId::INVALID), fixed_length_(INVALID_OID) {
    // Nothing to see...
  }

  Column(type::TypeId value_type, size_t column_length,
         std::string column_name, bool is_inlined = false,
         oid_t column_offset = INVALID_OID)
      : column_name(column_name),
        column_type_(value_type),
        fixed_length_(INVALID_OID),
        is_inlined_(is_inlined),
        column_offset_(column_offset) {
    SetInlined();

    // We should not have an inline value of length 0
    if (is_inlined && column_length == 0) {
      PELOTON_ASSERT(false);
    }

    SetLength(column_length);
  }

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // Set inlined
  void SetInlined();

  // Set the appropriate column length
  void SetLength(size_t column_length);

  oid_t GetOffset() const { return column_offset_; }

  std::string GetName() const { return column_name; }

  size_t GetLength() const {
    if (is_inlined_)
      return fixed_length_;
    else
      return variable_length_;
  }

  size_t GetFixedLength() const { return fixed_length_; }

  size_t GetVariableLength() const { return variable_length_; }

  inline type::TypeId GetType() const { return column_type_; }

  inline bool IsInlined() const { return is_inlined_; }

  inline bool IsPrimary() const { return is_primary_; }

  inline bool IsUnique() const { return is_unique_; }

  // Add a constraint to the column
  void AddConstraint(const catalog::Constraint &constraint) {
    if (constraint.GetType() == ConstraintType::DEFAULT) {
      // Add the default constraint to the front
      constraints_.insert(constraints_.begin(), constraint);
    } else {
      constraints_.push_back(constraint);
    }

    if (constraint.GetType() == ConstraintType::PRIMARY) {
      is_primary_ = true;
    }
    if (constraint.GetType() == ConstraintType::UNIQUE) {
      is_unique_ = true;
    }
  }

  const std::vector<Constraint> &GetConstraints() const { return constraints_; }

  hash_t Hash() const {
    hash_t hash = HashUtil::Hash(&column_type_);
    return HashUtil::CombineHashes(hash, HashUtil::Hash(&is_inlined_));
  }

  // Compare two column objects
  bool operator==(const Column &other) const {
    if (other.column_type_ != column_type_ || other.is_inlined_ != is_inlined_) {
      return false;
    }
    return true;
  }

  bool operator!=(const Column &other) const { return !(*this == other); }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // name of the column
  std::string column_name;

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

 private:
  // value type of column
  type::TypeId column_type_;  //  = type::TypeId::INVALID;

  // if the column is not inlined, this is set to pointer size
  // else, it is set to length of the fixed length column
  size_t fixed_length_;  //  = INVALID_OID;

  // if the column is inlined, this is set to 0
  // else, it is set to length of the variable length column
  size_t variable_length_ = 0;

  // is the column inlined ?
  bool is_inlined_ = false;

  // is the column contained the primary key?
  bool is_primary_ = false;

  // is the column unique
  bool is_unique_ = false;

  // offset of column in tuple
  oid_t column_offset_ = INVALID_OID;

  // Constraints
  std::vector<Constraint> constraints_;
};

}  // namespace catalog
}  // namespace peloton
