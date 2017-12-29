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

#include "common/internal_types.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Foreign Key Class
//===--------------------------------------------------------------------===//

// Stores info about foreign key constraints, like the sink table id etc.
class ForeignKey {
 public:
  ForeignKey(oid_t sink_table_id,
             std::vector<oid_t> sink_col_ids,
             std::vector<oid_t> source_col_ids,
             FKConstrActionType update_action,
             FKConstrActionType delete_action,
             std::string constraint_name)

      : sink_table_id(sink_table_id),
        sink_col_ids(sink_col_ids),
        source_col_ids(source_col_ids),
        update_action(update_action),
        delete_action(delete_action),
        fk_name(constraint_name) {}

  oid_t GetSinkTableOid() const { return sink_table_id; }

  std::vector<oid_t> GetSinkColumnIds() const { return sink_col_ids; }
  std::vector<oid_t> GetSourceColumnIds() const { return source_col_ids; }

  FKConstrActionType GetUpdateAction() const { return update_action; }
  FKConstrActionType GetDeleteAction() const { return delete_action; }
  std::string &GetConstraintName() { return fk_name; }

 private:
  oid_t sink_table_id = INVALID_OID;

  // Columns in the reference table (sink)
  std::vector<oid_t> sink_col_ids;

  // Columns in the current table (source)
  // Can be a single column or multiple columns depending
  // on the constraint
  std::vector<oid_t> source_col_ids;

  FKConstrActionType update_action;
  FKConstrActionType delete_action;

  std::string fk_name;
};

}  // namespace catalog
}  // namespace peloton
