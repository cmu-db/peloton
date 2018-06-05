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
  Column() : column_type(type::TypeId::INVALID), fixed_length(INVALID_OID) {
    // Nothing to see...
  }

  Column(type::TypeId value_type, size_t column_length,
         std::string column_name, bool is_inlined = false,
         oid_t column_offset = INVALID_OID)
      : column_name(column_name),
        column_type(value_type),
        fixed_length(INVALID_OID),
        is_inlined(is_inlined),
        column_offset(column_offset) {
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

  oid_t GetOffset() const { return column_offset; }

  std::string GetName() const { return column_name; }

  size_t GetLength() const {
    if (is_inlined)
      return fixed_length;
    else
      return variable_length;
  }

  size_t GetFixedLength() const { return fixed_length; }

  size_t GetVariableLength() const { return variable_length; }

  inline type::TypeId GetType() const { return column_type; }

  inline bool IsInlined() const { return is_inlined; }

  // Constraint check functions for NOT NULL and DEFAULT
  inline bool IsPrimary() const { return is_primary_; }

  inline bool IsUnique() const { return is_unique_; }

  inline bool IsNotNull() const { return is_not_null_; }

  inline bool IsDefault() const { return is_default_; }

  // Manage NOT NULL constraint
  void SetNotNull() { is_not_null_ = true; }

  void ClearNotNull() { is_not_null_ = false; }

  // Manage DEFAULT constraint
  void SetDefaultValue(std::shared_ptr<type::Value> value) {
  	default_value_ = value;
  	is_default_ = true;
  }

  inline std::shared_ptr<type::Value> GetDefaultValue() const {
  	return default_value_;
  }

  void ClearDefaultValue() {
  	default_value_.reset();
  	is_default_ = false; }

  // Add a constraint to the column
  void AddConstraint(const std::shared_ptr<Constraint> constraint) {
    if (constraint->GetType() == ConstraintType::DEFAULT) {
      // Add the default constraint to the front
      constraints.insert(constraints.begin(), constraint);
    } else {
      constraints.push_back(constraint);
    }

    if (constraint->GetType() == ConstraintType::PRIMARY) {
      is_primary_ = true;
    }
    if (constraint->GetType() == ConstraintType::UNIQUE) {
      is_unique_ = true;
    }
  }

  // Delete a constraint from the column
  bool DeleteConstraint(const oid_t constraint_oid);

  const std::vector<std::shared_ptr<Constraint>> &GetConstraints() const {
  	return constraints;
  }

  std::shared_ptr<Constraint> GetConstraint(oid_t constraint_oid) {
  	for (auto constraint : constraints) {
  		if (constraint->GetConstraintOid() == constraint_oid)
  			return constraint;
  	}
  	return nullptr;
  }

  hash_t Hash() const {
    hash_t hash = HashUtil::Hash(&column_type);
    return HashUtil::CombineHashes(hash, HashUtil::Hash(&is_inlined));
  }

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

  // name of the column
  std::string column_name;

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

 private:
  // value type of column
  type::TypeId column_type;  //  = type::TypeId::INVALID;

  // if the column is not inlined, this is set to pointer size
  // else, it is set to length of the fixed length column
  size_t fixed_length;  //  = INVALID_OID;

  // if the column is inlined, this is set to 0
  // else, it is set to length of the variable length column
  size_t variable_length = 0;

  // is the column inlined ?
  bool is_inlined = false;

  // is the column allowed null
  bool is_not_null_ = false;

  // is the column contained the default value
  bool is_default_ = false;

  // default value
  std::shared_ptr<type::Value> default_value_;

  // offset of column in tuple
  oid_t column_offset = INVALID_OID;
};

}  // namespace catalog
}  // namespace peloton
