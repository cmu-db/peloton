//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_value_expression.cpp
//
// Identification: src/expression/constant_value_expression.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/constant_value_expression.h"

namespace peloton {
namespace expression {

const std::string ConstantValueExpression::GetInfo(int num_indent) const {
  std::ostringstream os;

  os << StringUtil::Indent(num_indent) << "Expression ::\n"
     << StringUtil::Indent(num_indent + 1)
     << "expression type = Constant Value,\n"
     << StringUtil::Indent(num_indent + 1) << "value: " << value_.GetInfo()
     << std::endl;

  return os.str();
}

const std::string ConstantValueExpression::GetInfo() const {
  std::ostringstream os;
  os << GetInfo(0);

  return os.str();
}

}  // namespace expression
}  // namespace peloton
