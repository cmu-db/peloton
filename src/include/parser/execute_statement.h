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
      : SQLStatement(StatementType::EXECUTE) {}

  virtual ~ExecuteStatement() {}

  virtual void Accept(SqlNodeVisitor* v) override {
    v->Visit(this);
  }

  std::string name;
  std::vector<std::unique_ptr<expression::AbstractExpression>> parameters;
};

}  // namespace parser
}  // namespace peloton
