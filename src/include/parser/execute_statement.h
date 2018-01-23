//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// execute_statement.h
//
// Identification: src/include/parser/execute_statement.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"
#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"

namespace peloton {
namespace parser {

/**
 * @class ExecuteStatement
 * @brief Represents "EXECUTE ins_prep(100, "test", 2.3);"
 */
class ExecuteStatement : public SQLStatement {
 public:
  ExecuteStatement() : SQLStatement(StatementType::EXECUTE) {}

  virtual ~ExecuteStatement() {}

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

  std::string name;
  std::vector<std::unique_ptr<expression::AbstractExpression>> parameters;
};

}  // namespace parser
}  // namespace peloton
