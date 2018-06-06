//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_value_expression.h
//
// Identification: src/include/expression/parameter_value_expression.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"
#include "common/sql_node_visitor.h"
#include "util/hash_util.h"

namespace peloton {

namespace executor {
class ExecutorContext;
}  // namespace executor

namespace expression {

//===----------------------------------------------------------------------===//
// ParameterValueExpression
//===----------------------------------------------------------------------===//

class ParameterValueExpression : public AbstractExpression {
 public:
  ParameterValueExpression(int value_idx);

  int GetValueIdx() const { return value_idx_; }

  type::Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                       executor::ExecutorContext *context) const override;

  AbstractExpression *Copy() const override {
    return new ParameterValueExpression(value_idx_);
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  bool operator==(const AbstractExpression &rhs) const override;

  hash_t Hash() const override;

  bool IsNullable() const override { return is_nullable_; }

  void VisitParameters(
      codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override;

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

 protected:
  // Index of the value in the value vectors coming in from the user
  int value_idx_;

  // Nullability, updated after combining with value from the user
  bool is_nullable_;

  ParameterValueExpression(const ParameterValueExpression &other)
      : AbstractExpression(other), value_idx_(other.value_idx_) {}
};

}  // namespace expression
}  // namespace peloton
