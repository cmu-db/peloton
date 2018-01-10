//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// case_expression.cpp
//
// Identification: src/expression/case_expression.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/case_expression.h"

namespace peloton {
namespace expression {

const std::string CaseExpression::GetInfo(int num_indent) const {
  std::ostringstream os;

  os << StringUtil::Indent(num_indent) << "Expression ::\n"
     << StringUtil::Indent(num_indent + 1) << "expression type = Case,\n"
     << StringUtil::Indent(num_indent + 1) << "default: \n"
     << default_expr_.get()->GetInfo(num_indent + 2);

  if (clauses_.size() > 0) {
    os << StringUtil::Indent(num_indent + 1) << "clauses:\n";
    for (const auto &clause : clauses_) {
      os << StringUtil::Indent(num_indent + 2) << "first: \n"
         << clause.first.get()->GetInfo(num_indent + 3);
      os << StringUtil::Indent(num_indent + 2) << "second: \n"
         << clause.second.get()->GetInfo(num_indent + 3);
    }
  }

  return os.str();
}

const std::string CaseExpression::GetInfo() const {
  std::ostringstream os;
  os << GetInfo(0);

  return os.str();
}

}  // namespace expression
}  // namespace peloton
