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
#include "util/string_util.h"
#include "common/sql_node_visitor.h"
#include "common/internal_types.h"

namespace peloton {
namespace parser {

class SelectStatement;

// Definition of a join table
class JoinDefinition {
 public:
  JoinDefinition()
      : left(nullptr),
        right(nullptr),
        condition(nullptr),
        type(JoinType::INNER) {}

  virtual ~JoinDefinition() {}

  std::unique_ptr<TableRef> left;
  std::unique_ptr<TableRef> right;
  std::unique_ptr<expression::AbstractExpression> condition;

  JoinType type;

  void Accept(SqlNodeVisitor *v) { v->Visit(this); }
};

//  Holds reference to tables.
// Can be either table names or a select statement.
struct TableRef {
  TableRef(TableReferenceType type)
      : type(type), table_info_(nullptr), select(nullptr), join(nullptr) {}

  virtual ~TableRef();

  TableReferenceType type;

  std::string schema;

  // Expression of database name and table name
  std::unique_ptr<TableInfo> table_info_ = nullptr;

  std::string alias;

  SelectStatement *select;
  std::vector<std::unique_ptr<TableRef>> list;
  std::unique_ptr<JoinDefinition> join;

  // Convenience accessor methods
  inline bool HasSchema() { return !schema.empty(); }

  // Try to bind the database name to the node if not specified
  inline void TryBindDatabaseName(std::string default_database_name) {
    if (table_info_ == nullptr) {
      table_info_.reset(new TableInfo());
    }

    if (table_info_->database_name.empty()) {
      table_info_->database_name = default_database_name;
    }
  }

  // Get the name of the database of this table
  inline std::string GetDatabaseName() const {
    return table_info_->database_name;
  }

  // Get the name of the table
  inline std::string GetTableAlias() const {
    if (!alias.empty())
      return alias;
    else
      return table_info_->table_name;
  }

  inline std::string GetTableName() const { return table_info_->table_name; }

  const std::string GetInfo(int num_indent) const;

  void Accept(SqlNodeVisitor *v) { v->Visit(this); }
};

}  // namespace parser
}  // namespace peloton
