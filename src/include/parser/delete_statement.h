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
 * @class DeleteStatement
 * @brief Represents "DELETE FROM students WHERE grade > 3.0"
 *
 * If expr == NULL => delete all rows (truncate)
 */
class DeleteStatement : public SQLStatement {
 public:
  DeleteStatement()
      : SQLStatement(StatementType::DELETE),
        table_ref(nullptr), expr(nullptr) {};

  virtual ~DeleteStatement() {
    if (table_ref != nullptr) {
      delete table_ref;
    }

    if (expr != nullptr) {
      delete expr;
    }
  }

  std::string GetTableName() const {
    return table_ref->GetTableName();
  }

  std::string GetDatabaseName() const {
    return table_ref->GetDatabaseName();
  }

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  parser::TableRef* table_ref;
  expression::AbstractExpression* expr;
};

}  // End parser namespace
}  // End peloton namespace
