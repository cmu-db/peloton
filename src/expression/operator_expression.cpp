//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_expression.cpp
//
// Identification: src/expression/operator_expression.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/operator_expression.h"

namespace peloton {
namespace expression {

const std::string OperatorExpression::GetInfo(int num_indent) const {
  std::ostringstream os;

  os << StringUtil::Indent(num_indent) << "Expression ::\n"
     << StringUtil::Indent(num_indent + 1) << "expression type = Operator,\n"
     << StringUtil::Indent(num_indent + 1)
     << "operator name: " << ExpressionTypeToString(exp_type_) << std::endl;

  for (const auto &child : children_) {
    os << child.get()->GetInfo(num_indent + 2);
  }

  return os.str();
}

const std::string OperatorExpression::GetInfo() const {
  std::ostringstream os;
  os << GetInfo(0);

  return os.str();
}

}  // namespace expression
}  // namespace peloton
