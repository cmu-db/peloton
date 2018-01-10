//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// conjunction_expression.cpp
//
// Identification: src/expression/conjunction_expression.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/conjunction_expression.h"

namespace peloton {
namespace expression {

const std::string ConjunctionExpression::GetInfo(int num_indent) const {
  std::ostringstream os;

  os << StringUtil::Indent(num_indent) << "Expression ::\n"
     << StringUtil::Indent(num_indent + 1) << "expression type = Conjunction,\n"
     << StringUtil::Indent(num_indent + 1)
     << "conjunction type = " << ExpressionTypeToString(exp_type_) << "\n";

  for (const auto &child : children_) {
    os << child.get()->GetInfo(num_indent + 2);
  }

  return os.str();
}

const std::string ConjunctionExpression::GetInfo() const {
  std::ostringstream os;
  os << GetInfo(0);

  return os.str();
}

}  // namespace expression
}  // namespace peloton
