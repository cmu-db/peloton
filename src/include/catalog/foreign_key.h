//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// foreign_key.h
//
// Identification: src/include/catalog/foreign_key.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <string>
#include <vector>
#include <iostream>

#include "type/types.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Foreign Key Class
//===--------------------------------------------------------------------===//

// Stores info about foreign key constraints, like the sink table id etc.
class ForeignKey {
 public:
  ForeignKey(oid_t sink_table_id,
             std::vector<oid_t> pk_column_offsets,
             std::vector<oid_t> fk_column_offsets,
             FKConstrActionType fk_update_action,
             FKConstrActionType fk_delete_action,
             std::string constraint_name)

      : sink_table_id(sink_table_id),
        pk_column_names(pk_column_offsets),
        fk_column_names(fk_column_offsets),
        fk_update_action(fk_update_action),
        fk_delete_action(fk_delete_action),
        fk_name(constraint_name) {}

  oid_t GetSinkTableOid() const { return sink_table_id; }

  std::vector<oid_t> GetPKColumnNames() const { return pk_column_names; }
  std::vector<oid_t> GetFKColumnNames() const { return fk_column_names; }

  FKConstrActionType GetUpdateAction() const { return fk_update_action; }
  FKConstrActionType GetDeleteAction() const { return fk_delete_action; }
  std::string &GetConstraintName() { return fk_name; }

 private:
  oid_t sink_table_id = INVALID_OID;

  // Columns in the reference table (sink)
  std::vector<oid_t> pk_column_names;

  // Columns in the current table (source)
  // Can be a single column or multiple columns depending on the constraint
  std::vector<oid_t> fk_column_names;

  FKConstrActionType fk_update_action;
  FKConstrActionType fk_delete_action;

  std::string fk_name;
};

}  // End catalog namespace
}  // End peloton namespace
