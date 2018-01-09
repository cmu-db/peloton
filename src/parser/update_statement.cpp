//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_statement.cpp
//
// Identification: src/parser/update_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/update_statement.h"
#include "util/string_util.h"
#include <sstream>

namespace peloton {
namespace parser {

const std::string UpdateStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "UpdateStatement\n";
  os << table.get()->GetInfo(num_indent + 1) << std::endl;
  os << StringUtil::Indent(num_indent + 1) << "-> Updates :: " << std::endl;
  for (auto &update : updates) {
    os << StringUtil::Indent(num_indent + 2) << "Column: " << update->column
       << std::endl;
    os << update->value.get()->GetInfo(num_indent + 3) << std::endl;
  }
  if (where != nullptr) {
    os << StringUtil::Indent(num_indent + 1) << "-> Where :: " << std::endl
       << where.get()->GetInfo(num_indent + 2) << std::endl;
  }

  return os.str();
}

const std::string UpdateStatement::GetInfo() const {
  std::ostringstream os;

  os << "SQLStatement[UPDATE]\n";

  os << GetInfo(1);

  return os.str();
}

}  // namespace parser
}  // namespace peloton
