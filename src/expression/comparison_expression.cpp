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
