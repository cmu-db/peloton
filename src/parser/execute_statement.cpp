//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// execute_statement.cpp
//
// Identification: src/parser/execute_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/execute_statement.h"
#include "util/string_util.h"
#include <sstream>

namespace peloton {
namespace parser {

const std::string ExecuteStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "ExecuteStatement\n";
  os << StringUtil::Indent(num_indent + 1) << "Name: " << name << std::endl;
  for (const auto &parameter : parameters) {
    os << parameter.get()->GetInfo(num_indent + 1);
  }
  return os.str();
}

const std::string ExecuteStatement::GetInfo() const {
  std::ostringstream os;

  os << "SQLStatement[EXECUTE]\n";

  os << GetInfo(1);

  return os.str();
}

}  // namespace parser
}  // namespace peloton
