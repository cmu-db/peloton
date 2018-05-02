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
  PELOTON_ASSERT(children_.size() == 2);
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

bool ComparisonExpression::SymmetricsEquals(const AbstractExpression &other) const {
  PELOTON_ASSERT(children_.size() == 2);

  if (other.GetChildrenSize() != 2) {
    return false;
  }

  auto vcl = children_[0].get();
  auto vcr = children_[1].get();
  auto ocl = other.GetModifiableChild(0);
  auto ocr = other.GetModifiableChild(1);
  auto oexp_type_ = other.GetExpressionType();

  switch (exp_type_) {
    case (ExpressionType::COMPARE_EQUAL):
    case (ExpressionType::COMPARE_NOTEQUAL):
      return exp_type_ == oexp_type_ &&
             ((ocl->ExactlyEquals(*vcl) && ocr->ExactlyEquals(*vcr)) ||
	      (ocl->ExactlyEquals(*vcr) && ocr->ExactlyEquals(*vcl)));
    case (ExpressionType::COMPARE_LESSTHAN):
      return (ocl->ExactlyEquals(*vcl) && ocr->ExactlyEquals(*vcr)
	      && oexp_type_ == ExpressionType::COMPARE_LESSTHAN) ||
	     (ocl->ExactlyEquals(*vcr) && ocr->ExactlyEquals(*vcl)
	      && oexp_type_ == ExpressionType::COMPARE_GREATERTHAN);
    case (ExpressionType::COMPARE_GREATERTHAN):
      return (ocl->ExactlyEquals(*vcl) && ocr->ExactlyEquals(*vcr)
	      && oexp_type_ == ExpressionType::COMPARE_GREATERTHAN) ||
	     (ocl->ExactlyEquals(*vcr) && ocr->ExactlyEquals(*vcl)
	      && oexp_type_ == ExpressionType::COMPARE_LESSTHAN);
    case (ExpressionType::COMPARE_LESSTHANOREQUALTO):
      return (ocl->ExactlyEquals(*vcl) && ocr->ExactlyEquals(*vcr)
	      && oexp_type_ == ExpressionType::COMPARE_LESSTHANOREQUALTO) ||
	     (ocl->ExactlyEquals(*vcr) && ocr->ExactlyEquals(*vcl)
	      && oexp_type_ == ExpressionType::COMPARE_GREATERTHANOREQUALTO);
    case (ExpressionType::COMPARE_GREATERTHANOREQUALTO):
      return (ocl->ExactlyEquals(*vcl) && ocr->ExactlyEquals(*vcr)
	      && oexp_type_ == ExpressionType::COMPARE_GREATERTHANOREQUALTO) ||
	     (ocl->ExactlyEquals(*vcr) && ocr->ExactlyEquals(*vcl)
	      && oexp_type_ == ExpressionType::COMPARE_LESSTHANOREQUALTO);
    case (ExpressionType::COMPARE_DISTINCT_FROM):
      return exp_type_ == oexp_type_ &&
             ocl->ExactlyEquals(*vcl) && ocr->ExactlyEquals(*vcr);
    default:
      throw Exception("Invalid comparison expression type.");
  }  
}

}  // namespace expression
}  // namespace peloton
