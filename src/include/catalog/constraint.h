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
  // Constructor for primary key or unique
  Constraint(oid_t constraint_oid, ConstraintType type,
             std::string constraint_name, oid_t table_oid,
             std::vector<oid_t> column_ids, oid_t index_oid)
      : constraint_oid(constraint_oid), constraint_name(constraint_name),
        constraint_type(type), table_oid(table_oid),
        column_ids(column_ids), index_oid(index_oid) {}

  // Constructor for foreign key constraint
  Constraint(oid_t constraint_oid, ConstraintType type,
             std::string constraint_name, oid_t table_oid,
             std::vector<oid_t> column_ids,oid_t index_oid, oid_t sink_table_oid,
             std::vector<oid_t> sink_col_ids, FKConstrActionType update_action,
             FKConstrActionType delete_action)
      : constraint_oid(constraint_oid), constraint_name(constraint_name),
        constraint_type(type), table_oid(table_oid),
        column_ids(column_ids), index_oid(index_oid),
        fk_sink_table_oid(sink_table_oid), fk_sink_col_ids(sink_col_ids),
        fk_update_action(update_action), fk_delete_action(delete_action) {}

  // Constructor for check constraint
  Constraint(oid_t constraint_oid, ConstraintType type,
             std::string constraint_name, oid_t table_oid,
             std::vector<oid_t> column_ids, oid_t index_oid,
             std::pair<ExpressionType, type::Value> exp)
      : constraint_oid(constraint_oid),  constraint_name(constraint_name),
        constraint_type(type), table_oid(table_oid), column_ids(column_ids),
        index_oid(index_oid), check_exp(exp) {}

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // Set oid for catalog
  void SetConstraintOid(oid_t oid) { constraint_oid = oid; }

  inline oid_t GetConstraintOid() const { return constraint_oid; }

  inline ConstraintType GetType() const { return constraint_type; }

  // Get the table oid
  inline oid_t GetTableOid() const { return table_oid; }

  // Get the column ids
  inline const std::vector<oid_t> &GetColumnIds() const { return column_ids; }

  // Set index oid indicating the index constructing the constraint
  void SetIndexOid(oid_t oid) { index_oid = oid; }

  // Get the index oid
  inline oid_t GetIndexOid() const { return index_oid; }

  inline std::string GetName() const { return constraint_name; }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  inline oid_t GetFKSinkTableOid() const { return fk_sink_table_oid; }

  inline const std::vector<oid_t> &GetFKSinkColumnIds() const { return fk_sink_col_ids; }

  inline FKConstrActionType GetFKUpdateAction() const { return fk_update_action; }

  inline FKConstrActionType GetFKDeleteAction() const { return fk_delete_action; }

  inline std::pair<ExpressionType, type::Value> GetCheckExpression() const { return check_exp; }

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // constraint oid created by catalog
  oid_t constraint_oid;

  std::string constraint_name;

  // The type of constraint
  ConstraintType constraint_type;

  // Table having this constraints
  oid_t table_oid;

  // Column ids related the constraint
  std::vector<oid_t> column_ids;

  // Index constructing the constraint
  oid_t index_oid;

  // foreign key constraint information
  // The reference table (sink)
  oid_t fk_sink_table_oid = INVALID_OID;

  // Column ids in the reference table (sink)
  std::vector<oid_t> fk_sink_col_ids;

  FKConstrActionType fk_update_action = FKConstrActionType::NOACTION;

  FKConstrActionType fk_delete_action = FKConstrActionType::NOACTION;

  // key string is column name for check constraint
  std::pair<ExpressionType, type::Value> check_exp;
};

}  // namespace catalog
}  // namespace peloton
