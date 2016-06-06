//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_manager.h
//
// Identification: src/include/optimizer/column_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/column.h"

namespace peloton {
namespace optimizer {

class ColumnManager {
 public:
  ColumnManager();
  ~ColumnManager();

  Column *LookupColumn(oid_t base_table, oid_t column_index);

  Column *LookupColumnByID(ColumnID id);

  Column *AddBaseColumn(ValueType type, int size, std::string name,
                        bool inlined, oid_t base_table, oid_t column_index);

  Column *AddExprColumn(ValueType type, int size, std::string name,
                        bool inlined);

 private:
  ColumnID next_column_id;

  std::vector<Column *> columns;
  std::map<std::tuple<oid_t, oid_t>, Column *> table_col_index_to_column;
  std::map<ColumnID, Column *> id_to_column;
};

} /* namespace optimizer */
} /* namespace peloton */
