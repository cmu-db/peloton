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

namespace peloton {
namespace expression {

void AbstractExpression::DeduceExpressionName() {
  // If alias exists, it will be used in TrafficCop
  if (!alias.empty()) return;

  for (auto& child : children_) child->DeduceExpressionName();
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

void AbstractExpression::SetValueType(type::Type::TypeId type_id) {
  if (return_value_type_ == type::Type::PARAMETER_OFFSET) {
    return_value_type_ = type_id;
  }
}

void AbstractExpression::SetValueType() {
  if (!HasParameter()) {
    return;
  }
  switch (this->GetExpressionType()) {
    case ExpressionType::VALUE_PARAMETER:
    case ExpressionType::VALUE_CONSTANT:
    case ExpressionType::VALUE_TUPLE: {
      break;
    }
    case ExpressionType::COMPARE_EQUAL:
    case ExpressionType::COMPARE_NOTEQUAL:
    case ExpressionType::COMPARE_LESSTHAN:
    case ExpressionType::COMPARE_GREATERTHAN:
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
      return_value_type_ = type::Type::BOOLEAN;
      if (children_.size() == 2) {
        auto type_left = children_[0].get()->GetValueType();
        auto type_right = children_[1].get()->GetValueType();
        children_[0].get()->SetValueType(type_right);
        children_[1].get()->SetValueType(type_left);
      }
      break;
    }
    case ExpressionType::CONJUNCTION_AND:
    case ExpressionType::CONJUNCTION_OR: {
      return_value_type_ = type::Type::BOOLEAN;
      if (children_.size() == 2) {
        children_[0].get()->SetValueType(type::Type::BOOLEAN);
        children_[1].get()->SetValueType(type::Type::BOOLEAN);
      }
      break;
    }
    case ExpressionType::OPERATOR_PLUS:
    case ExpressionType::OPERATOR_MINUS:
    case ExpressionType::OPERATOR_MULTIPLY:
    case ExpressionType::OPERATOR_DIVIDE:
    case ExpressionType::OPERATOR_MOD: {
      if (children_.size() == 2) {
        auto type_left = children_[0].get()->GetValueType();
        auto type_right = children_[1].get()->GetValueType();
        children_[0].get()->SetValueType(type_right);
        children_[1].get()->SetValueType(type_left);
        return_value_type_ = children_[0].get()->GetValueType();
      }
      break;
    }
    case ExpressionType::OPERATOR_UNARY_MINUS: {
      if (return_value_type_ != type::Type::INVALID && children_.size() == 1) {
        children_[0].get()->SetValueType(return_value_type_);
      }
      break;
    }
        /*
        case ExpressionType::OPERATOR_CASE_EXPR: {
          // TODO: Uncomment me when we have case
          auto &case_exp = static_cast<const expression::CaseExpression &>(exp);
          translator = new CaseTranslator(case_exp, context);
          break;
        }
        */
    default: {
      throw Exception{"Unexpected expression type: " +
                      ExpressionTypeToString(GetExpressionType())};
    }
  }
}
}  // namespace expression
}  // namespace peloton