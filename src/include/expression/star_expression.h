//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_expression.h
//
// Identification: src/include/expression/function_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/sql_node_visitor.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// StarExpression
//===----------------------------------------------------------------------===//

class StarExpression : public AbstractExpression {
 public:
  StarExpression() : AbstractExpression(ExpressionType::STAR) {}

  type::Value Evaluate(
      UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    return type::ValueFactory::GetBooleanValue(true);
  }

  AbstractExpression *Copy() const override {
    return new StarExpression(*this);
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override {
    std::ostringstream os;

    os << StringUtil::Indent(num_indent) << "Expression ::\n"
       << StringUtil::Indent(num_indent + 1) << "expression type = Star,\n";

    return os.str();
  }

  const std::string GetInfo() const override {
    std::ostringstream os;
    os << GetInfo(0);

    return os.str();
  }

 protected:
  StarExpression(const AbstractExpression &other) : AbstractExpression(other) {}

 private:
};

}  // namespace expression
}  // namespace peloton
