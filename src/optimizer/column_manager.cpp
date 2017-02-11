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
#include "parser/table_ref.h"
#include "expression/tuple_value_expression.h"

#include <tuple>
#include "catalog/catalog.h"

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

Column *ColumnManager::AddBaseColumn(type::Type::TypeId type, int size, std::string name,
                                     bool inlined, oid_t base_table,
                                     oid_t column_index) {
  LOG_TRACE(
      "Adding base column: %s, type %d, size %d, inlined %s, "
      "table %u, col %u",
      name.c_str(), type, size,
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

Column *ColumnManager::AddExprColumn(type::Type::TypeId type, int size, std::string name,
                                     bool inlined) {
  LOG_TRACE("Adding expr column: %s, type %d, size %d, inlined %s",
            name.c_str(), type, size,
            inlined ? "yes" : "no");
  Column *col = new ExprColumn(next_column_id++, type, size, name, inlined);

  id_to_column.insert(std::pair<ColumnID, Column *>(col->ID(), col));
  columns.push_back(col);
  return col;
}

void ColumnManager::AddTable(oid_t db_id, oid_t table_id, const parser::TableRef* table_ref) {
  auto id_tuple = std::make_tuple(db_id, table_id);
  table_id_tuples.insert(id_tuple);

  std::string alias;
  if (table_ref->alias != nullptr) {
    alias = table_ref->alias;
  }
  else {
    alias = table_ref->GetTableName();
  }

  if (table_alias_to_id_tuple.find(alias) != table_alias_to_id_tuple.end()) {
    throw Exception("Duplicate alias");
  }
  table_alias_to_id_tuple[alias] = id_tuple;

}

Column* ColumnManager::BindColumnRefToColumn(expression::TupleValueExpression* col_expr) {
  auto table_ref_name = col_expr->GetTableName();
  oid_t schema_col_id = (oid_t)-1;
  oid_t table_id = (oid_t)-1;

  // Table alias or name is not specified
  if (table_ref_name.empty()) {
    // TODO: Check ambiguous column name in different tables
    for (auto& id_tuple: table_id_tuples) {
      auto schema = catalog::Catalog::GetInstance()->
          GetTableWithOid(std::get<0>(id_tuple), std::get<1>(id_tuple))->
          GetSchema();
      schema_col_id = schema->GetColumnID(col_expr->GetColumnName();
      if (schema_col_id != (oid_t) -1) {
        table_id = std::get<1>(id_tuple);
        break;
      }
    }
  }
  else {
    auto id_tuple_iter = table_alias_to_id_tuple.find(table_ref_name);
    if (id_tuple_iter != table_alias_to_id_tuple.end()) {
      auto id_tuple = (*id_tuple_iter).second;
      auto schema = catalog::Catalog::GetInstance()->
          GetTableWithOid(std::get<0>(id_tuple), std::get<1>(id_tuple))->
          GetSchema();
      table_id = std::get<0>(id_tuple);
      schema_col_id = schema->GetColumnID(col_expr->GetColumnName();
    }
  }

  if (schema_col_id == (oid_t)-1)
    return nullptr;

  return LookupColumn(table_id, schema_col_id);

}

} /* namespace optimizer */
} /* namespace peloton */
