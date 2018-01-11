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
  /**
   * Comparison expression constructor
   *
   * @param type The exact type of comparison (e.g., less-than, greater-than)
   * @param left The left side of the comparison
   * @param right The right side of the comparison
   */
  ComparisonExpression(ExpressionType type, AbstractExpression *left,
                       AbstractExpression *right);

  /**
   * Perform the comparison of the first and second provided input tuples.
   *
   * @param tuple1 The left tuple
   * @param tuple2 The right tuple
   * @param context A context object
   * @return The result of the comparison
   */
  type::Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                       executor::ExecutorContext *context) const override;

  /**
   * Copy this comparison expression into a new expression object. This method
   * relinquishes ownership of the object after creation. It is the caller's
   * responsibility to clean up what is returned.
   *
   * @return An exact copy of this comparison expression
   */
  AbstractExpression *Copy() const override;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline ComparisonExpression::ComparisonExpression(ExpressionType type,
                                                  AbstractExpression *left,
                                                  AbstractExpression *right)
    : AbstractExpression(type, type::TypeId::BOOLEAN, left, right) {}

inline type::Value ComparisonExpression::Evaluate(
    const AbstractTuple *tuple1, const AbstractTuple *tuple2,
    executor::ExecutorContext *context) const {
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
      return type::ValueFactory::GetBooleanValue(vl.CompareLessThanEquals(vr));
    case (ExpressionType::COMPARE_GREATERTHANOREQUALTO):
      return type::ValueFactory::GetBooleanValue(
          vl.CompareGreaterThanEquals(vr));
    case (ExpressionType::COMPARE_DISTINCT_FROM): {
      if (vl.IsNull() && vr.IsNull()) {
        return type::ValueFactory::GetBooleanValue(false);
      } else if (!vl.IsNull() && !vr.IsNull()) {
        return type::ValueFactory::GetBooleanValue(vl.CompareNotEquals(vr));
      }
      return type::ValueFactory::GetBooleanValue(true);
    }
    default:
      throw Exception("Invalid comparison expression type.");
  }
}

inline AbstractExpression *ComparisonExpression::Copy() const {
  return new ComparisonExpression(GetExpressionType(), GetChild(0)->Copy(),
                                  GetChild(1)->Copy());
}

}  // namespace expression
}  // namespace peloton
