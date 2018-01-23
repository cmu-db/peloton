//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_expression.h
//
// Identification: src/include/expression/conjunction_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"
#include "common/sql_node_visitor.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// OperatorExpression
//===----------------------------------------------------------------------===//

class ConjunctionExpression : public AbstractExpression {
 public:
  ConjunctionExpression(ExpressionType type) : AbstractExpression(type) {}

  ConjunctionExpression(ExpressionType type, AbstractExpression *left,
                        AbstractExpression *right)
      : AbstractExpression(type, type::TypeId::BOOLEAN, left, right) {}

  type::Value Evaluate(
      UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    PL_ASSERT(children_.size() == 2);
    auto vl = children_[0]->Evaluate(tuple1, tuple2, context);
    auto vr = children_[1]->Evaluate(tuple1, tuple2, context);
    switch (exp_type_) {
      case (ExpressionType::CONJUNCTION_AND): {
        if (vl.IsTrue() && vr.IsTrue())
          return type::ValueFactory::GetBooleanValue(true);
        if (vl.IsFalse() || vr.IsFalse())
          return type::ValueFactory::GetBooleanValue(false);
        return type::ValueFactory::GetBooleanValue(type::PELOTON_BOOLEAN_NULL);
      }
      case (ExpressionType::CONJUNCTION_OR): {
        if (vl.IsFalse() && vr.IsFalse())
          return type::ValueFactory::GetBooleanValue(false);
        if (vl.IsTrue() || vr.IsTrue())
          return type::ValueFactory::GetBooleanValue(true);
        return type::ValueFactory::GetBooleanValue(type::PELOTON_BOOLEAN_NULL);
      }
      default:
        throw Exception("Invalid conjunction expression type.");
    }
  }

  AbstractExpression *Copy() const override {
    return new ConjunctionExpression(*this);
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

 protected:
  ConjunctionExpression(const ConjunctionExpression &other)
      : AbstractExpression(other) {}
};

}  // namespace expression
}  // namespace peloton
