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

}  // namespace expression
}  // namespace peloton
