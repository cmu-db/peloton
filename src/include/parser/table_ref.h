//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_ref.h
//
// Identification: src/include/parser/table_ref.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdio.h>
#include <vector>

#include "expression/abstract_expression.h"
#include "parser/sql_statement.h"
#include "common/sql_node_visitor.h"
#include "type/types.h"

namespace peloton {
namespace parser {

class SelectStatement;
class JoinDefinition;

//  Holds reference to tables.
// Can be either table names or a select statement.
struct TableRef {
  TableRef(TableReferenceType type)
      : type(type),
        table_info_(nullptr),
        select(nullptr),
        join(nullptr) {}

  virtual ~TableRef();

  TableReferenceType type;

  std::string schema;

  // Expression of database name and table name
  std::unique_ptr<TableInfo> table_info_ = nullptr;

  std::string alias;

  SelectStatement* select;
  std::vector<std::unique_ptr<TableRef>> list;
  std::unique_ptr<JoinDefinition> join;

  // Convenience accessor methods
  inline bool HasSchema() { return !schema.empty(); }

  // Get the name of the database of this table
  inline std::string GetDatabaseName() const {
    if (table_info_ == nullptr || table_info_->database_name.empty()) {
      return DEFAULT_DB_NAME;
    }
    return table_info_->database_name;
  }

  // Get the name of the table
  inline std::string GetTableAlias() const {
    if (!alias.empty())
      return alias;
    else
      return table_info_->table_name;
  }

  inline std::string GetTableName() const {
    return table_info_->table_name;
  }

  void Accept(SqlNodeVisitor* v) const { v->Visit(this); }
};

// Definition of a join table
class JoinDefinition {
 public:
  JoinDefinition()
      : left(nullptr), right(nullptr), condition(nullptr), type(JoinType::INNER) {}

  virtual ~JoinDefinition() {}

  std::unique_ptr<TableRef> left;
  std::unique_ptr<TableRef> right;
  std::unique_ptr<expression::AbstractExpression> condition;

  JoinType type;

  void Accept(SqlNodeVisitor* v) const { v->Visit(this); }
};

}  // namespace parser
}  // namespace peloton
