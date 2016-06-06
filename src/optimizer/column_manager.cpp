//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_manager.cpp
//
// Identification: src/optimizer/column_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/column_manager.h"

#include <tuple>

namespace peloton {
namespace optimizer {

ColumnManager::ColumnManager() {}

ColumnManager::~ColumnManager() {
  for (Column *col : columns) {
    delete col;
  }
}

Column *ColumnManager::LookupColumn(oid_t base_table, oid_t column_index) {
  auto key = std::make_tuple(base_table, column_index);
  auto it = table_col_index_to_column.find(key);
  if (it == table_col_index_to_column.end()) {
    return nullptr;
  } else {
    return it->second;
  }
}

Column *ColumnManager::LookupColumnByID(ColumnID id) {
  return id_to_column.at(id);
}

Column *ColumnManager::AddBaseColumn(ValueType type, int size, std::string name,
                                     bool inlined, oid_t base_table,
                                     oid_t column_index) {
  LOG_TRACE(
      "Adding base column: %s, type %s, size %d, inlined %s, "
      "table %lu, col %lu",
      name.c_str(), ValueTypeToString(type).c_str(), size,
      inlined ? "yes" : "no", base_table, column_index);
  Column *col = new TableColumn(next_column_id++, type, size, name, inlined,
                                base_table, column_index);

  auto key = std::make_tuple(base_table, column_index);
  table_col_index_to_column.insert(
      std::pair<decltype(key), Column *>(key, col));
  id_to_column.insert(std::pair<ColumnID, Column *>(col->ID(), col));
  columns.push_back(col);
  return col;
}

Column *ColumnManager::AddExprColumn(ValueType type, int size, std::string name,
                                     bool inlined) {
  LOG_TRACE("Adding expr column: %s, type %s, size %d, inlined %s",
            name.c_str(), ValueTypeToString(type).c_str(), size,
            inlined ? "yes" : "no");
  Column *col = new ExprColumn(next_column_id++, type, size, name, inlined);

  id_to_column.insert(std::pair<ColumnID, Column *>(col->ID(), col));
  columns.push_back(col);
  return col;
}

} /* namespace optimizer */
} /* namespace peloton */
