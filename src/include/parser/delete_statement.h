//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_statement.h
//
// Identification: src/include/parser/delete_statement.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
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

  virtual ~DeleteStatement() {}

  std::string GetTableName() const {
    return table_ref->GetTableName();
  }

  std::string GetDatabaseName() const {
    return table_ref->GetDatabaseName();
  }

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  std::unique_ptr<parser::TableRef> table_ref;
  std::unique_ptr<expression::AbstractExpression> expr;
};

}  // End parser namespace
}  // End peloton namespace
