//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constraint.h
//
// Identification: src/include/catalog/constraint.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <string>
#include <utility>
#include <vector>

#include "common/printable.h"
#include "common/internal_types.h"
#include "type/value.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Constraint Class
//===--------------------------------------------------------------------===//

class Constraint : public Printable {
 public:
  Constraint(ConstraintType type, std::string constraint_name)
      : constraint_type_(type), constraint_name_(std::move(constraint_name)) {}
  Constraint(ConstraintType type, std::string constraint_name,
             std::string check_cmd)
      : constraint_type_(type),
        constraint_name_(std::move(constraint_name)),
        check_cmd_(std::move(check_cmd)) {}

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  ConstraintType GetType() const { return constraint_type_; }

  std::pair<ExpressionType, type::Value> GetCheckExpression() { return exp_; }

  // Offset into the list of "reference tables" in the Table.
  void SetForeignKeyListOffset(oid_t offset) { fk_list_offset_ = offset; }

  // Offset into the list of "unique indices" in the Table.
  void SetUniqueIndexOffset(oid_t offset) {
    unique_index_list_offset_ = offset;
  }

  // Get the offset
  oid_t GetForeignKeyListOffset() const { return fk_list_offset_; }

  // Get the offset
  oid_t GetUniqueIndexOffset() const { return unique_index_list_offset_; }

  std::string GetName() const { return constraint_name_; }

  // Get a string representation for debugging
  const std::string GetInfo() const override;

  // Todo: default union data structure,
  // For default constraint
  void addDefaultValue(const type::Value &value) {
    if (constraint_type_ != ConstraintType::DEFAULT
        || default_value_.get() != nullptr) return;
    default_value_.reset(new peloton::type::Value(value));
  }

  type::Value *getDefaultValue() {
    return default_value_.get();
  }

  // Add check constrain
  void AddCheck(ExpressionType op, peloton::type::Value val) {
    exp_ = std::pair<ExpressionType, type::Value>(op, val);
  };

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The type of constraint
  ConstraintType constraint_type_ = ConstraintType::INVALID;

  // Offsets into the Unique index and reference table lists in Table
  oid_t fk_list_offset_ = INVALID_OID;

  oid_t unique_index_list_offset_ = INVALID_OID;

  std::string constraint_name_;

  std::shared_ptr<type::Value> default_value_;

  std::string check_cmd_ = "";

  // key string is column name
  std::pair<ExpressionType, type::Value> exp_;
};

}  // namespace catalog
}  // namespace peloton
