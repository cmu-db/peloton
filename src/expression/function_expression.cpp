//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_expression.cpp
//
// Identification: /peloton/src/expression/function_expression.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/function_expression.h"

namespace peloton {
namespace expression {

const std::string FunctionExpression::GetInfo(int num_indent) const {
  std::ostringstream os;

  os << StringUtil::Indent(num_indent) << "Expression ::\n"
     << StringUtil::Indent(num_indent + 1) << "expression type = Function,\n"
     << StringUtil::Indent(num_indent + 1) << "function name: " << func_name_
     << "\n" << StringUtil::Indent(num_indent + 1)
     << "function args: " << std::endl;

  for (const auto &child : children_) {
    os << child.get()->GetInfo(num_indent + 2);
  }

  return os.str();
}

const std::string FunctionExpression::GetInfo() const {
  std::ostringstream os;
  os << GetInfo(0);

  return os.str();
}

}  // namespace expression
}  // namespace peloton
