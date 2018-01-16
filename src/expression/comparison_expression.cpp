//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// comparison_expression.cpp
//
// Identification: /peloton/src/expression/comparison_expression.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/comparison_expression.h"

namespace peloton {
namespace expression {

ComparisonExpression::ComparisonExpression(ExpressionType type,
                                           AbstractExpression *left,
                                           AbstractExpression *right)
    : AbstractExpression(type, type::TypeId::BOOLEAN, left, right) {}

type::Value ComparisonExpression::Evaluate(
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

AbstractExpression *ComparisonExpression::Copy() const {
  return new ComparisonExpression(GetExpressionType(), GetChild(0)->Copy(),
                                  GetChild(1)->Copy());
}

const std::string ComparisonExpression::GetInfo(int num_indent) const {
  std::ostringstream os;

  os << StringUtil::Indent(num_indent) << "Expression ::\n"
     << StringUtil::Indent(num_indent + 1) << "expression type = Comparison,\n"
     << StringUtil::Indent(num_indent + 1)
     << "comparison type = " << ExpressionTypeToString(exp_type_) << "\n";

  for (const auto &child : children_) {
    os << child.get()->GetInfo(num_indent + 2);
  }

  return os.str();
}

const std::string ComparisonExpression::GetInfo() const {
  std::ostringstream os;
  os << GetInfo(0);

  return os.str();
}

}  // namespace expression
}  // namespace peloton
