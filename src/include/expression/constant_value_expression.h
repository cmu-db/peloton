//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_value_expression.h
//
// Identification: src/include/expression/constant_value_expression.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "type/value.h"
#include "util/hash_util.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// ConstantValueExpression
// Represents a constant value like int and string.
//===----------------------------------------------------------------------===//

class ConstantValueExpression : public AbstractExpression {
 public:
  explicit ConstantValueExpression(const type::Value &value);

  // Evaluate this expression
  type::Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                       executor::ExecutorContext *context) const override;

  void DeduceExpressionName() override;

  // Hashing
  hash_t HashForExactMatch() const override;
  hash_t Hash() const override;

  // Equality checks
  bool ExactlyEquals(const AbstractExpression &other) const override;
  bool operator==(const AbstractExpression &rhs) const override;
  bool operator!=(const AbstractExpression &rhs) const override {
    return !(*this == rhs);
  }

  void VisitParameters(
      codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override;

  // Copy this constant expression
  AbstractExpression *Copy() const override;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  // Value accessor
  type::Value GetValue() const { return value_; }

  // Constant value expressions do not have a parameter
  bool HasParameter() const override { return false; }

  // Is the expression nullable? In this case, it's whether the constnat value
  // is null or not.
  bool IsNullable() const override { return value_.IsNull(); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

 private:
  // The constant
  type::Value value_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline ConstantValueExpression::ConstantValueExpression(
    const type::Value &value)
    : AbstractExpression(ExpressionType::VALUE_CONSTANT, value.GetTypeId()),
      value_(value.Copy()) {}

inline type::Value ConstantValueExpression::Evaluate(
    UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
    UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
    UNUSED_ATTRIBUTE executor::ExecutorContext *context) const {
  return value_;
}

inline void ConstantValueExpression::DeduceExpressionName() {
  if (!alias.empty()) return;
  expr_name_ = value_.ToString();
}

inline bool ConstantValueExpression::ExactlyEquals(
    const AbstractExpression &other) const {
  if (exp_type_ != other.GetExpressionType()) return false;
  auto &const_expr = static_cast<const ConstantValueExpression &>(other);
  return value_.CompareEquals(const_expr.value_) == CmpBool::TRUE;
}

inline hash_t ConstantValueExpression::HashForExactMatch() const {
  hash_t hash = HashUtil::Hash(&exp_type_);
  return HashUtil::CombineHashes(hash, value_.Hash());
}

inline hash_t ConstantValueExpression::Hash() const {
  // Use VALUE_PARAMETER for parameterization with the compiled query cache
  auto val = ExpressionType::VALUE_PARAMETER;
  hash_t hash = HashUtil::Hash(&val);

  // Do not check value since we are going to parameterize and cache
  // but, check the nullability to optimize the non-nullable cases
  auto is_nullable = value_.IsNull();
  return HashUtil::CombineHashes(hash, HashUtil::Hash(&is_nullable));
}

inline bool ConstantValueExpression::operator==(
    const AbstractExpression &rhs) const {
  auto type = rhs.GetExpressionType();

  if ((type != ExpressionType::VALUE_PARAMETER) &&
      (exp_type_ != type || return_value_type_ != rhs.GetValueType())) {
    return false;
  }

  auto &other = static_cast<const expression::ConstantValueExpression &>(rhs);
  // Do not check value since we are going to parameterize and cache
  // but, check the nullability to optimize the non-nullable cases
  if (value_.IsNull() != other.IsNullable()) {
    return false;
  }

  return true;
}

inline void ConstantValueExpression::VisitParameters(
    codegen::QueryParametersMap &map, std::vector<peloton::type::Value> &values,
    UNUSED_ATTRIBUTE const std::vector<peloton::type::Value> &
        values_from_user) {
  auto is_nullable = value_.IsNull();
  map.Insert(Parameter::CreateConstParameter(value_.GetTypeId(), is_nullable),
             this);
  values.push_back(value_);
}

inline AbstractExpression *ConstantValueExpression::Copy() const {
  return new ConstantValueExpression(GetValue());
}

}  // namespace expression
}  // namespace peloton
