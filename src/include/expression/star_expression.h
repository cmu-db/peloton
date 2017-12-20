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

 protected:
  StarExpression(const AbstractExpression &other) : AbstractExpression(other) {}

 private:
};

}  // namespace expression
}  // namespace peloton
