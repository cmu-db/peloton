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

#include "expression/abstract_expression.h"

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
      : AbstractExpression(type, type::Type::BOOLEAN) {}

  ComparisonExpression(ExpressionType type, AbstractExpression *left,
                       AbstractExpression *right)
      : AbstractExpression(type, type::Type::BOOLEAN, left, right) {}

  type::Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
                 UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
                 UNUSED_ATTRIBUTE executor::ExecutorContext *context) const
      override {
    PL_ASSERT(children_.size() == 2);
    auto vl = children_[0]->Evaluate(tuple1, tuple2, context);
    auto vr = children_[1]->Evaluate(tuple1, tuple2, context);
    switch (exp_type_) {
      case(EXPRESSION_TYPE_COMPARE_EQUAL) :
        return vl.CompareEquals(vr);
      case(EXPRESSION_TYPE_COMPARE_NOTEQUAL) :
        return vl.CompareNotEquals(vr);
      case(EXPRESSION_TYPE_COMPARE_LESSTHAN) :
        return vl.CompareLessThan(vr);
      case(EXPRESSION_TYPE_COMPARE_GREATERTHAN) :
        return vl.CompareGreaterThan(vr);
      case(EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO) :
        return vl.CompareLessThanEquals(vr);
      case(EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO) :
        return vl.CompareGreaterThanEquals(vr);
      default:
        throw Exception("Invalid comparison expression type.");
    }
  }

  AbstractExpression *Copy() const override {
    return new ComparisonExpression(*this);
  }

 protected:
  ComparisonExpression(const ComparisonExpression &other)
      : AbstractExpression(other) {}
};

}  // End expression namespace
}  // End peloton namespace
