//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_drop.h
//
// Identification: src/include/parser/statement_drop.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"
#include "common/sql_node_visitor.h"

namespace peloton {
namespace parser {

/**
 * @class DropStatement
 * @brief Represents "DROP TABLE"
 */
class DropStatement : public TableRefStatement {
 public:
  enum EntityType {
    kDatabase,
    kTable,
    kSchema,
    kIndex,
    kView,
    kPreparedStatement,
    kTrigger
  };

  DropStatement(EntityType type)
      : TableRefStatement(StatementType::DROP), type(type), missing(false) {}

  virtual ~DropStatement() {
    if (database_name != nullptr) {
      delete[] database_name;
    }

    if (index_name != nullptr) {
      delete[] index_name;
    }

    if (prep_stmt != nullptr) {
      delete[] prep_stmt;
    }

    if (table_name != nullptr) {
      delete[] table_name;
    }

    if (trigger_name != nullptr) {
      delete[] trigger_name;
    }
  }

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  EntityType type;
  char* database_name = nullptr;
  char* index_name = nullptr;
  char* prep_stmt = nullptr;
  bool missing;

  // drop trigger
  char* table_name = nullptr;
  char* trigger_name = nullptr;
};

}  // namespace parser
}  // namespace peloton
