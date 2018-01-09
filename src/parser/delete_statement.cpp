//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_statement.cpp
//
// Identification: src/parser/delete_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/delete_statement.h"
#include "util/string_util.h"
#include <sstream>

namespace peloton {
namespace parser {

const std::string DeleteStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "DeleteStatement\n";
  os << StringUtil::Indent(num_indent + 1) << GetTableName();
  if (expr != nullptr) os << expr.get()->GetInfo(num_indent + 1) << std::endl;
  return os.str();
}

const std::string DeleteStatement::GetInfo() const {
  std::ostringstream os;
  os << "SQLStatement[DELETE]\n";
  os << GetInfo(1);

  return os.str();
}

}  // namespace parser
}  // namespace peloton
