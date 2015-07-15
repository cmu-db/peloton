/*-------------------------------------------------------------------------
 *
 * constraint.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/constraint.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <string>
#include <vector>
#include <iostream>

#include "backend/common/types.h"
#include "backend/common/logger.h"

#include "nodes/nodes.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// ReferenceTableInfo Class
//===--------------------------------------------------------------------===//

// Represent the sink tables of foreign key constraints
class ReferenceTableInfo {

 public:
  ReferenceTableInfo(oid_t reference_table_id,
                     std::vector<std::string> pk_column_names,
                     std::vector<std::string> fk_column_names,
                     char fk_update_action,
                     char fk_delete_action,
                     std::string constraint_name)

 : reference_table_id(reference_table_id),
   pk_column_names(pk_column_names),
   fk_column_names(fk_column_names),
   fk_update_action(fk_update_action),
   fk_delete_action(fk_delete_action),
   constraint_name(constraint_name) {
  }

  std::vector<std::string> GetFKColumnNames() {
    return fk_column_names;
  }

  oid_t GetReferenceTableId() {
    return reference_table_id;
  }

  std::string GetName() {
    return constraint_name;
  }

 private:

  oid_t reference_table_id = INVALID_OID;

  // Columns in the reference table (sink)
  std::vector<std::string> pk_column_names;

  // Columns in the current table (source)
  // Can be a single column or multiple columns depending on the constraint
  std::vector<std::string> fk_column_names;

  // What to do when foreign key is updated or deleted ?
  // FIXME: Not used in our executors currently
  char fk_update_action;
  char fk_delete_action;

  std::string constraint_name;

};

//===--------------------------------------------------------------------===//
// Constraint Class
//===--------------------------------------------------------------------===//

class Constraint {

 public:

  Constraint(ConstraintType type,
             std::string constraint_name = "",
             Node* raw_expr = nullptr)
 : constraint_type(type),
   constraint_name(constraint_name) {

    // Copy the raw expression
    raw_expr = (Node*) copyObject((void*) raw_expr );

  }

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  ConstraintType GetType() const {
    return constraint_type;
  }

  // Offset into the list of "reference tables" in the Table.
  inline void SetReferenceTablePosition(oid_t position) {
    reference_table_list_offset = position;
  }

  // Offset into the list of "unique indices" in the Table.
  inline void SetUniqueIndexPosition(oid_t position ) {
    unique_index_list_offset = position;
  }

  std::string GetName() const {
    return constraint_name;
  }

  // Get a string representation of this constraint
  friend std::ostream& operator<<(std::ostream& os, const Constraint& constraint);

 private:

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The type of constraint
  ConstraintType constraint_type = CONSTRAINT_TYPE_INVALID;

  // Raw_default_expr
  // FIXME :: Cooked expr
  Node* raw_expr = nullptr;

  // Offsets into the Unique index and reference table lists in Table
  oid_t reference_table_list_offset = INVALID_OID;

  oid_t unique_index_list_offset = INVALID_OID;

  std::string constraint_name;

};


} // End catalog namespace
} // End peloton namespace
