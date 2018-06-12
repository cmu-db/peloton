//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_value_expression.cpp
//
// Identification: src/expression/parameter_value_expression.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/parameter_value_expression.h"

#include "executor/executor_context.h"

namespace peloton {
namespace expression {

ParameterValueExpression::ParameterValueExpression(int value_idx)
    : AbstractExpression(ExpressionType::VALUE_PARAMETER,
                         type::TypeId::PARAMETER_OFFSET),
      value_idx_(value_idx),
      is_nullable_(false) {}

type::Value ParameterValueExpression::Evaluate(
    UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
    UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
    executor::ExecutorContext *context) const {
  return context->GetParamValues().at(value_idx_);
}

bool ParameterValueExpression::operator==(const AbstractExpression &rhs) const {
  auto type = rhs.GetExpressionType();
  if ((type != ExpressionType::VALUE_CONSTANT) &&
      (exp_type_ != type || return_value_type_ != rhs.GetValueType())) {
    return false;
  }

  auto &other = static_cast<const expression::ParameterValueExpression &>(rhs);
  // Do not check value since we are going to parameterize and cache
  // but, check the nullability for optimizing the non-nullable cases
  if (is_nullable_ != other.IsNullable()) {
    return false;
  }

  return true;
}

hash_t ParameterValueExpression::Hash() const {
  hash_t hash = HashUtil::Hash(&exp_type_);

  // Do not hash value since we are going to parameterize and cache
  // but, check the nullability for optimizing the non-nullable cases
  return HashUtil::CombineHashes(hash, HashUtil::Hash(&is_nullable_));
}

void ParameterValueExpression::VisitParameters(
    codegen::QueryParametersMap &map, std::vector<peloton::type::Value> &values,
    const std::vector<peloton::type::Value> &values_from_user) {
  // Add a new parameter object for a parameter
  auto &value = values_from_user[value_idx_];

  // We update nullability from the value and keep this in expression
  is_nullable_ = value.IsNull();
  map.Insert(Parameter::CreateParamParameter(value.GetTypeId(), is_nullable_),
             this);
  values.push_back(value);
  return_value_type_ = value.GetTypeId();
};

const std::string ParameterValueExpression::GetInfo(int num_indent) const {
  std::ostringstream os;

  os << StringUtil::Indent(num_indent) << "Expression ::\n"
     << StringUtil::Indent(num_indent + 1)
     << "expression type = Parameter Value,\n"
     << StringUtil::Indent(num_indent + 1)
     << "value index: " << std::to_string(value_idx_)
     << StringUtil::Indent(num_indent + 1)
     << "nullable: " << ((is_nullable_) ? "True" : "False") << std::endl;

  return os.str();
}

const std::string ParameterValueExpression::GetInfo() const {
  std::ostringstream os;
  os << GetInfo(0);

  return os.str();
}

}  // namespace expression
}  // namespace peloton
