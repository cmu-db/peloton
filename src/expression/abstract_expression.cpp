//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_expression.cpp
//
// Identification: /peloton/src/expression/abstract_expression.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include "expression/abstract_expression.h"
#include "util/hash_util.h"
#include "expression/expression_util.h"

namespace peloton {
namespace expression {

void AbstractExpression::DeduceExpressionName() {
  // If alias exists, it will be used in TrafficCop
  if (!alias.empty()) return;

  for (auto& child : children_) child->DeduceExpressionName();

  // Aggregate expression already has correct expr_name_
  if (ExpressionUtil::IsAggregateExpression(exp_type_))
    return;

  auto op_str = ExpressionTypeToString(exp_type_, true);
  auto children_size = children_.size();
  PL_ASSERT(children_size <= 2);
  if (children_size == 2) {
    expr_name_ =
        GetChild(0)->expr_name_ + " " + op_str + " " + GetChild(1)->expr_name_;
  } else if (children_size == 1) {
    expr_name_ = op_str + " " + GetChild(0)->expr_name_;
  }
}

const std::string AbstractExpression::GetInfo() const {
  std::ostringstream os;

  os << "\tExpression :: "
     << " expression type = " << GetExpressionType() << ","
     << " value type = " << type::Type::GetInstance(GetValueType())->ToString()
     << "," << std::endl;

  return os.str();
}

bool AbstractExpression::Equals(AbstractExpression* expr) const{
  if (exp_type_ != expr->exp_type_ ||
      children_.size() != expr->children_.size())
    return false;
  for (unsigned i = 0; i < children_.size(); i++) {
    if (!children_[i]->Equals(expr->children_[i].get())) return false;
  }
  return true;
}

hash_t AbstractExpression::Hash() const {
  hash_t hash = HashUtil::Hash(&exp_type_);
  for (size_t i = 0; i < GetChildrenSize(); i++) {
    auto child = GetChild(i);
    hash = HashUtil::CombineHashes(hash, child->Hash());
  }
  return hash;
}

}  // namespace expression
}  // namespace peloton