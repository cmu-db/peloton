#pragma once

#include "backend/common/types.h"

#include <vector>

namespace peloton {
namespace bridge {

class raw_foreign_key_info {

public:
  raw_foreign_key_info(oid_t source_table_id,
                       oid_t sink_table_id,
                       std::vector<int> source_column_offsets,
                       std::vector<int> sink_column_offsets,
                       char update_action,
                       char delete_action,
                       std::string fk_name)
                       : source_table_id(source_table_id),
                         sink_table_id(sink_table_id),
                         source_column_offsets(source_column_offsets),
                         sink_column_offsets(sink_column_offsets),
                         update_action(update_action),
                         delete_action(delete_action),
                         fk_name(fk_name) {}

  void CreateForeignkey(void) const;

private:

  oid_t source_table_id;  // a table that has a reference key
  oid_t sink_table_id;    // a table that has a primary key

  std::vector<int> source_column_offsets;
  std::vector<int> sink_column_offsets;

  // Refer:: http://www.postgresql.org/docs/9.4/static/catalog-pg-constraint.html
  // foreign key action
  char update_action;
  char delete_action;

  std::string fk_name;
};

}  // namespace bridge
}  // namespace peloton
