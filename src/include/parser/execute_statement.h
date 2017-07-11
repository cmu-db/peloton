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
 * @class ExecuteStatement
 * @brief Represents "EXECUTE ins_prep(100, "test", 2.3);"
 */
class ExecuteStatement : public SQLStatement {
 public:
  ExecuteStatement()
      : SQLStatement(StatementType::EXECUTE), name(NULL), parameters(NULL) {}

  virtual ~ExecuteStatement() {
    if (name != nullptr) {
      delete[] name;
    }

    if (parameters) {
      for (auto expr : *parameters) {
        if (expr != nullptr) {
          delete expr;
        }
      }
    }

    if (parameters != nullptr) {
      delete parameters;
    }
  }

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  char* name;
  std::vector<expression::AbstractExpression*>* parameters;
};

}  // End parser namespace
}  // End peloton namespace
