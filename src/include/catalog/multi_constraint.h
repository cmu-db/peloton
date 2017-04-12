//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// multi_constraint.h
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
#include "type/types.h"
#include "type/value.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Constraint Class
//===--------------------------------------------------------------------===//

class MultiConstraint : public Printable {
 public:
  MultiConstraint(ConstraintType type, std::string constraint_name)
      : constraint_type(type), constraint_name(constraint_name){};

  MultiConstraint(ConstraintType type, std::string constraint_name,
                  std::vector<oid_t> column_ids)
      : constraint_type(type), constraint_name(constraint_name) {
    this->column_ids = column_ids;
  };

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  ConstraintType GetType() const { return constraint_type; }

  std::string GetName() const { return constraint_name; }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  std::vector<oid_t> GetCols() const { return column_ids; }

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The type of constraint
  ConstraintType constraint_type = ConstraintType::INVALID;

  // constraints on column set
  std::vector<oid_t> column_ids;

  // we do not allow duplicate constraint name in single table
  std::string constraint_name;
};

}  // End catalog namespace
}  // End peloton namespace
