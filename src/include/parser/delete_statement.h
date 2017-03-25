//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_delete.h
//
// Identification: src/include/parser/statement_delete.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/logger.h"
#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"

namespace peloton {
namespace parser {

/**
 * @struct DeleteStatement
 * @brief Represents "DELETE FROM students WHERE grade > 3.0"
 *
 * If expr == NULL => delete all rows (truncate)
 */
struct DeleteStatement : SQLStatement {
  DeleteStatement()
      : SQLStatement(StatementType::DELETE),
        table_ref(nullptr), expr(nullptr) {};

  virtual ~DeleteStatement() {
    delete table_ref;
    delete table_info_;
    delete expr;
  }

  std::string GetTableName() const { return table_ref->GetTableName(); }

  std::string GetDatabaseName() const {
    return table_ref->GetDatabaseName();
  }

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  parser::TableRef* table_ref;
  parser::TableInfo* table_info_ = nullptr;
  expression::AbstractExpression* expr;
};

}  // End parser namespace
}  // End peloton namespace
