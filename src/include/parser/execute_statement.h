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

  const std::string GetInfo(int num_indent) const {
    std::ostringstream os;
    os << StringUtil::Indent(num_indent) << "ExecuteStatement\n";
    os << StringUtil::Indent(num_indent + 1) << "Name: " << name << "\n";
    for (const auto& parameter: parameters) {
      os << StringUtil::Indent(num_indent + 1) << parameter.get()->GetInfo() << "\n";
    }
    return os.str();
  }

  const std::string GetInfo() const {
    std::ostringstream os;

    os << "SQLStatement[EXECUTE]\n";

    os << GetInfo(1);

    return os.str();
  }

  std::string name;
  std::vector<std::unique_ptr<expression::AbstractExpression>> parameters;
};

}  // namespace parser
}  // namespace peloton
