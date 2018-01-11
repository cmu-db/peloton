//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// array_expression.h
//
// Identification: src/include/expression/array_expression.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/postgresparser.h"
#include "type/value_factory.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// ArrayExpression
//===----------------------------------------------------------------------===//

class ArrayExpression : public AbstractExpression {
 public:
  ArrayExpression(const std::vector<AbstractExpression *> &expr_array, 
    const type::Value &value)
      : AbstractExpression(ExpressionType::ARRAY, value.GetTypeId()),
      value_(value.Copy()) {
    for (auto &expr : expr_array) {
      expr_array_.push_back(std::unique_ptr<AbstractExpression>(expr));
    }
  }

  type::Value Evaluate(
      UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    return type::ValueFactory::GetBooleanValue(true);
  }

  AbstractExpression *Copy() const override {
    return new ArrayExpression(*this);
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  type::Value GetValue() const { return value_; }

 protected:
  ArrayExpression(const ArrayExpression &other)
      : AbstractExpression(other) {}

  std::vector<std::unique_ptr<expression::AbstractExpression>> expr_array_;
  type::Value value_;
};

}  // namespace expression
}  // namespace peloton
