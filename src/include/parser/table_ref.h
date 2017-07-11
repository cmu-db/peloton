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
        schema(NULL),
        table_info_(NULL),
        alias(NULL),
        select(NULL),
        list(NULL),
        join(NULL) {}

  virtual ~TableRef();

  TableReferenceType type;

  char* schema;

  // Expression of database name and table name
  TableInfo* table_info_ = nullptr;

  char* alias;

  SelectStatement* select;
  std::vector<TableRef*>* list;
  JoinDefinition* join;

  // Convenience accessor methods
  inline bool HasSchema() { return schema != NULL; }

  // Get the name of the database of this table
  inline const char* GetDatabaseName() const {
    if (table_info_ == nullptr || table_info_->database_name == nullptr) {
      return DEFAULT_DB_NAME;
    }
    return table_info_->database_name;
  }

  // Get the name of the table
  inline const char* GetTableAlias() const {
    if (alias != NULL)
      return alias;
    else
      return table_info_->table_name;
  }

  inline const char* GetTableName() const { return table_info_->table_name; }

  void Accept(SqlNodeVisitor* v) const { v->Visit(this); }
};

// Definition of a join table
class JoinDefinition {
 public:
  JoinDefinition()
      : left(NULL), right(NULL), condition(NULL), type(JoinType::INNER) {}

  virtual ~JoinDefinition() {
    delete left;
    delete right;
    delete condition;
  }

  TableRef* left;
  TableRef* right;
  expression::AbstractExpression* condition;

  JoinType type;

  void Accept(SqlNodeVisitor* v) const { v->Visit(this); }
};

}  // End parser namespace
}  // End peloton namespace
