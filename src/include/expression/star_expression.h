//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// star_expression.h
//
// Identification: src/include/expression/star_expression.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"

#include <string>

#include "common/sql_node_visitor.h"
#include "type/value_factory.h"

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

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

 protected:
  StarExpression(const AbstractExpression &other) : AbstractExpression(other) {}

 private:
};

}  // namespace expression
}  // namespace peloton
