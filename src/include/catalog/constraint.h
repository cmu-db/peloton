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
  Constraint(ConstraintType type, std::string constraint_name,
  		oid_t index_oid = INVALID_OID)
      : constraint_type(type), constraint_name(constraint_name),
				index_oid(index_oid) {}

  Constraint(ConstraintType type, std::string constraint_name,
             std::string check_cmd, oid_t index_oid = INVALID_OID)
      : constraint_type(type), constraint_name(constraint_name),
				index_oid(index_oid), check_cmd(check_cmd) {}

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  void SetConstraintOid(oid_t oid) { constraint_oid = oid; }

  oid_t GetConstraintOid() const { return constraint_oid; }

  ConstraintType GetType() const { return constraint_type; }

  // Set index oid indicating the index constructing the constraint
  void SetIndexOid(oid_t oid) { index_oid = oid; }

  // Get the index oid
  oid_t GetIndexOid() const { return index_oid; }

  std::string GetName() const { return constraint_name; }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Todo: default union data structure,
  // For default constraint
  void addDefaultValue(const type::Value &value) {
    if (constraint_type != ConstraintType::DEFAULT || default_value.get() != nullptr) {
      return;
    }

    default_value.reset(new peloton::type::Value(value));
  }

  type::Value* getDefaultValue() const {
    return default_value.get();
  }

  // Add check constrain
  void AddCheck(ExpressionType op, peloton::type::Value val) {
    exp = std::pair<ExpressionType, type::Value>(op, val);
    return;
  };

  std::pair<ExpressionType, type::Value> GetCheckExpression() const { return exp; }

  std::string GetCheckCmd() const { return check_cmd; }

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  oid_t constraint_oid = INVALID_OID;

  // The type of constraint
  ConstraintType constraint_type = ConstraintType::INVALID;

  std::string constraint_name;

  // The index constructing the constraint
  oid_t index_oid;

  std::shared_ptr<type::Value> default_value;

  std::string check_cmd = "";

  // key string is column name
  std::pair<ExpressionType, type::Value> exp;
};

}  // namespace catalog
}  // namespace peloton
