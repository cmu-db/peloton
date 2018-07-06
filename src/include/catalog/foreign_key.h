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
  ForeignKey(oid_t source_table_id,
             oid_t sink_table_id,
             std::vector<oid_t> sink_col_ids,
             std::vector<oid_t> source_col_ids,
             FKConstrActionType update_action,
             FKConstrActionType delete_action,
             std::string constraint_name)

      : source_table_id_(source_table_id),
        sink_table_id_(sink_table_id),
        sink_col_ids_(sink_col_ids),
        source_col_ids_(source_col_ids),
        update_action_(update_action),
        delete_action_(delete_action),
        fk_name_(constraint_name) {}

  oid_t GetSourceTableOid() const { return source_table_id_; }
  oid_t GetSinkTableOid() const { return sink_table_id_; }

  std::vector<oid_t> GetSinkColumnIds() const { return sink_col_ids_; }
  std::vector<oid_t> GetSourceColumnIds() const { return source_col_ids_; }

  FKConstrActionType GetUpdateAction() const { return update_action_; }
  FKConstrActionType GetDeleteAction() const { return delete_action_; }
  std::string &GetConstraintName() { return fk_name_; }

 private:
  oid_t source_table_id_ = INVALID_OID;
  oid_t sink_table_id_ = INVALID_OID;

  // Columns in the reference table (sink)
  std::vector<oid_t> sink_col_ids_;

  // Columns in the current table (source)
  // Can be a single column or multiple columns depending
  // on the constraint
  std::vector<oid_t> source_col_ids_;

  FKConstrActionType update_action_;
  FKConstrActionType delete_action_;

  std::string fk_name_;
};

}  // namespace catalog
}  // namespace peloton
