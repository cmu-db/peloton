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

#include "expression/abstract_expression.h"

#include <string>

#include "codegen/type/type.h"
#include "util/hash_util.h"
#include "expression/expression_util.h"

namespace peloton {
namespace expression {

bool AbstractExpression::HasParameter() const {
  for (auto &child : children_) {
    if (child->HasParameter()) {
      return true;
    }
  }
  return false;
}

bool AbstractExpression::IsNullable() const {
  for (const auto &child : children_) {
    if (child->IsNullable()) {
      return true;
    }
  }
  return false;
}

void AbstractExpression::PerformBinding(
    const std::vector<const planner::BindingContext *> &binding_contexts) {
  // Most expressions don't need attribute binding, except for those
  // that actually reference table attributes (i.e., TVE)
  for (uint32_t i = 0; i < GetChildrenSize(); i++) {
    children_[i]->PerformBinding(binding_contexts);
  }
}

void AbstractExpression::GetUsedAttributes(
    std::unordered_set<const planner::AttributeInfo *> &attributes) const {
  for (uint32_t i = 0; i < GetChildrenSize(); i++) {
    children_[i]->GetUsedAttributes(attributes);
  }
}

codegen::type::Type AbstractExpression::ResultType() const {
  return codegen::type::Type{GetValueType(), IsNullable()};
}

void AbstractExpression::DeduceExpressionName() {
  // If alias exists, it will be used in TrafficCop
  if (!alias.empty()) return;

  for (auto& child : children_) child->DeduceExpressionName();

  // Aggregate expression already has correct expr_name_
  if (ExpressionUtil::IsAggregateExpression(exp_type_))
    return;

  auto op_str = ExpressionTypeToString(exp_type_, true);
  auto children_size = children_.size();
  if (exp_type_ == ExpressionType::FUNCTION) {
    auto expr = (FunctionExpression*)this;
    expr_name_ = expr->GetFuncName() + "(";
    for (size_t i = 0; i < children_size; i++) {
      if (i > 0) expr_name_.append(",");
      expr_name_.append(GetChild(i)->expr_name_);
    }
    expr_name_.append(")");
  }
  else {
    PL_ASSERT(children_size <= 2);
    if (children_size == 2) {
      expr_name_ =
          GetChild(0)->expr_name_ + " " + op_str + " " + GetChild(1)->expr_name_;
    } else if (children_size == 1) {
      expr_name_ = op_str + " " + GetChild(0)->expr_name_;
    }
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

bool AbstractExpression::IsZoneMappable() {
  bool is_zone_mappable = ExpressionUtil::GetPredicateForZoneMap(parsed_predicates,Copy());
  return is_zone_mappable;
}



}  // namespace expression
}  // namespace peloton