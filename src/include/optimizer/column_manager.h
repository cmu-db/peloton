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

#include <map>
#include <set>

#include "optimizer/column.h"
#include "parser/table_ref.h"
#include "expression/tuple_value_expression.h"

namespace peloton {
namespace optimizer {

// TODO: Refactor
class ColumnManager {
 public:
  ColumnManager();
  ~ColumnManager();

  Column *LookupColumn(oid_t base_table, oid_t column_index);

  Column *LookupColumnByID(ColumnID id);

  Column *AddBaseColumn(type::Type::TypeId type, int size, std::string name,
                        bool inlined, oid_t base_table, oid_t column_index);

  Column *AddExprColumn(type::Type::TypeId type, int size, std::string name,
                        bool inlined);

  void AddTable(oid_t db_id, oid_t table_id, const parser::TableRef* table_ref);

  Column* BindColumnRefToColumn(expression::TupleValueExpression* col_expr);


 private:
  ColumnID next_column_id = 0;


  std::vector<Column *> columns;
  std::map<std::tuple<oid_t, oid_t>, Column *> table_col_index_to_column;
  std::map<ColumnID, Column *> id_to_column;

  // Set of the table ids in the current query context
  std::set<std::tuple<oid_t, oid_t>> table_id_tuples;

  // When alias is not set, set its table name
  std::map<std::string, std::tuple<oid_t, oid_t>> table_alias_to_id_tuple;
};

} /* namespace optimizer */
} /* namespace peloton */
