//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// star_expression.cpp
//
// Identification: src/expression/star_expression.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/star_expression.h"
#include "util/string_util.h"
#include <sstream>

namespace peloton {
namespace expression {

const std::string StarExpression::GetInfo(int num_indent) const {
  std::ostringstream os;

  os << StringUtil::Indent(num_indent) << "Expression ::\n"
     << StringUtil::Indent(num_indent + 1) << "expression type = Star,"
     << std::endl;

  return os.str();
}

const std::string StarExpression::GetInfo() const {
  std::ostringstream os;
  os << GetInfo(0);

  return os.str();
}

}  // namespace expression
}  // namespace peloton
