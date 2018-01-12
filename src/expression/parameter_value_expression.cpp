//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_value_expression.cpp
//
// Identification: src/expression/parameter_value_expression.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/parameter_value_expression.h"

namespace peloton {
namespace expression {

const std::string ParameterValueExpression::GetInfo(int num_indent) const {
  std::ostringstream os;

  os << StringUtil::Indent(num_indent) << "Expression ::\n"
     << StringUtil::Indent(num_indent + 1)
     << "expression type = Parameter Value,\n"
     << StringUtil::Indent(num_indent + 1)
     << "value index: " << std::to_string(value_idx_)
     << StringUtil::Indent(num_indent + 1)
     << "nullable: " << ((is_nullable_) ? "True" : "False") << std::endl;

  return os.str();
}

const std::string ParameterValueExpression::GetInfo() const {
  std::ostringstream os;
  os << GetInfo(0);

  return os.str();
}

}  // namespace expression
}  // namespace peloton
