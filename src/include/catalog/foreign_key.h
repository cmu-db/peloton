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
  ForeignKey(const std::string &sink_table_name,
             std::vector<std::string> pk_column_names,
             std::vector<std::string> fk_column_names,
             char fk_update_action, char fk_delete_action,
             std::string constraint_name)

      : sink_table_name(sink_table_name),
        pk_column_names(pk_column_names),
        fk_column_names(fk_column_names),
        fk_update_action(fk_update_action),
        fk_delete_action(fk_delete_action),
        fk_name(constraint_name) {}

  std::string GetSinkTableName() const { return sink_table_name; }

  std::vector<std::string> GetPKColumnNames() const { return pk_column_names; }
  std::vector<std::string> GetFKColumnNames() const { return fk_column_names; }

  char GetUpdateAction() const { return fk_update_action; }

  char GetDeleteAction() const { return fk_delete_action; }

  std::string &GetConstraintName() { return fk_name; }

 private:
  std::string sink_table_name;

  // Columns in the reference table (sink)
  std::vector<std::string> pk_column_names;

  // Columns in the current table (source)
  // Can be a single column or multiple columns depending on the constraint
  std::vector<std::string> fk_column_names;

  char fk_update_action;
  char fk_delete_action;

  std::string fk_name;
};

}  // End catalog namespace
}  // End peloton namespace
