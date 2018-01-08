//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_ref.cpp
//
// Identification: src/parser/prepare_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/prepare_statement.h"

namespace peloton {
namespace parser {

const std::string PrepareStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "PrepareStatement\n";
  os << StringUtil::Indent(num_indent + 1) << "Name: " << name << "\n";
  os << query.get()->GetInfo(num_indent + 1) << "\n";
  for (const auto &placeholder : placeholders) {
    os << placeholder.get()->GetInfo(num_indent + 1) << "\n";
  }
  return os.str();
}

const std::string PrepareStatement::GetInfo() const {
  std::ostringstream os;

  os << "SQLStatement[PREPARE]\n";

  os << GetInfo(1);

  return os.str();
}

}  // namespace parser
}  // namespace peloton
