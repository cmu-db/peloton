//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_statement.cpp
//
// Identification: src/parser/copy_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/copy_statement.h"

namespace peloton {
namespace parser {

const std::string CopyStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "CopyStatement\n";
  os << StringUtil::Indent(num_indent + 1)
     << "-> Type :: " << CopyTypeToString(type) << "\n";
  os << cpy_table.get()->GetInfo(num_indent + 1) << std::endl;

  os << StringUtil::Indent(num_indent + 1) << "-> File Path :: " << file_path
     << std::endl;
  os << StringUtil::Indent(num_indent + 1) << "-> Delimiter :: " << delimiter;
  os << std::endl;
  return os.str();
}

const std::string CopyStatement::GetInfo() const {
  std::ostringstream os;

  os << "SQLStatement[COPY]\n";

  os << GetInfo(1);

  return os.str();
}

}  // namespace parser
}  // namespace peloton
