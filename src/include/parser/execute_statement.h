//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_execute.h
//
// Identification: src/include/parser/statement_execute.h
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
 * @struct ExecuteStatement
 * @brief Represents "EXECUTE ins_prep(100, "test", 2.3);"
 */
struct ExecuteStatement : SQLStatement {
  ExecuteStatement()
      : SQLStatement(StatementType::EXECUTE), name(NULL), parameters(NULL) {}

  virtual ~ExecuteStatement() {
    free (name);

    if (parameters) {
      for (auto expr : *parameters) delete expr;
    }

    delete parameters;
  }

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  char* name;
  std::vector<expression::AbstractExpression*>* parameters;
};

}  // End parser namespace
}  // End peloton namespace
