//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// comparison_expression.h
//
// Identification: src/include/expression/comparison_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "type/value_factory.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// ComparisonExpression
//===----------------------------------------------------------------------===//

class ComparisonExpression : public AbstractExpression {
 public:
  // TODO: Should we delete left and right if they are not nullptr?
  ~ComparisonExpression() {}

  ComparisonExpression(ExpressionType type)
      : AbstractExpression(type, type::TypeId::BOOLEAN) {}

  ComparisonExpression(ExpressionType type, AbstractExpression *left,
                       AbstractExpression *right)
      : AbstractExpression(type, type::TypeId::BOOLEAN, left, right) {}

  type::Value Evaluate(
      const AbstractTuple *tuple1,
      const AbstractTuple *tuple2,
      executor::ExecutorContext *context) const override {
    PL_ASSERT(children_.size() == 2);
    auto vl = children_[0]->Evaluate(tuple1, tuple2, context);
    auto vr = children_[1]->Evaluate(tuple1, tuple2, context);
    switch (exp_type_) {
      case (ExpressionType::COMPARE_EQUAL):
        return type::ValueFactory::GetBooleanValue(vl.CompareEquals(vr));
      case (ExpressionType::COMPARE_NOTEQUAL):
        return type::ValueFactory::GetBooleanValue(vl.CompareNotEquals(vr));
      case (ExpressionType::COMPARE_LESSTHAN):
        return type::ValueFactory::GetBooleanValue(vl.CompareLessThan(vr));
      case (ExpressionType::COMPARE_GREATERTHAN):
        return type::ValueFactory::GetBooleanValue(vl.CompareGreaterThan(vr));
      case (ExpressionType::COMPARE_LESSTHANOREQUALTO):
        return type::ValueFactory::GetBooleanValue(
            vl.CompareLessThanEquals(vr));
      case (ExpressionType::COMPARE_GREATERTHANOREQUALTO):
        return type::ValueFactory::GetBooleanValue(
            vl.CompareGreaterThanEquals(vr));
      case (ExpressionType::COMPARE_DISTINCT_FROM): {
        if (vl.IsNull() && vr.IsNull()) {
          return type::ValueFactory::GetBooleanValue(false);
        }
        else if (!vl.IsNull() && !vr.IsNull()) {
          return type::ValueFactory::GetBooleanValue(vl.CompareNotEquals(vr));
        }
        return type::ValueFactory::GetBooleanValue(true);
      }
      default:
        throw Exception("Invalid comparison expression type.");
    }
  }

  AbstractExpression *Copy() const override {
    return new ComparisonExpression(*this);
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

 protected:
  ComparisonExpression(const ComparisonExpression &other)
      : AbstractExpression(other) {}
};

}  // End expression namespace
}  // End peloton namespace
