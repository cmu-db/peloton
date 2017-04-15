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
#include <map>

#include "common/printable.h"
#include "type/type.h"
#include "type/types.h"
#include "type/value.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Constraint Class
//===--------------------------------------------------------------------===//

class Constraint : public Printable {
 public:
  Constraint(ConstraintType type, std::string constraint_name)
      : constraint_type(type), constraint_name(constraint_name) {}
  Constraint(ConstraintType type, std::string constraint_name,
             std::string check_cmd)
      : constraint_type(type),
        constraint_name(constraint_name),
        check_cmd(check_cmd) {}
  
  // typedef std::pair<ExpressionType, type::Value> ExpConstrain;

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  ConstraintType GetType() const { return constraint_type; }

  std::pair<ExpressionType, type::Value> GetExp() { return exp; }

  // Offset into the list of "reference tables" in the Table.
  void SetForeignKeyListOffset(oid_t offset) { fk_list_offset = offset; }

  // Offset into the list of "unique indices" in the Table.
  void SetUniqueIndexOffset(oid_t offset) { unique_index_list_offset = offset; }

  // Get the offset
  oid_t GetForeignKeyListOffset() const { return fk_list_offset; }

  // Get the offset
  oid_t GetUniqueIndexOffset() const { return unique_index_list_offset; }

  std::string GetName() const { return constraint_name; }

  // Get check command
  std::string GetcheckInfo() const {
    std::ostringstream os;
    os << exp.first << " " << exp.second.GetInfo().c_str();
    return os.str();
  }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Todo: default union data structure,
  // For default constraint
  void addDefaultValue(const type::Value &value) {
    if (constraint_type != ConstraintType::DEFAULT || default_value != nullptr) {
      return;
    }

    default_value = new peloton::type::Value(value);
  }

  type::Value* getDefaultValue() {
    return default_value;
  }

  // Add check constrain
  void AddCheck(ExpressionType op, peloton::type::Value val) {
    exp = std::pair<ExpressionType, type::Value>(op, val);
    return;
  };

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The type of constraint
  ConstraintType constraint_type = ConstraintType::INVALID;

  // Offsets into the Unique index and reference table lists in Table
  oid_t fk_list_offset = INVALID_OID;

  oid_t unique_index_list_offset = INVALID_OID;

  std::string constraint_name;

  type::Value *default_value = nullptr;

  std::string check_cmd = "";

  // key string is column name
  std::pair<ExpressionType, type::Value> exp;
};

}  // End catalog namespace
}  // End peloton namespace
