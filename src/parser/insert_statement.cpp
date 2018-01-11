//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_statement.cpp
//
// Identification: src/parser/insert_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/insert_statement.h"
#include "parser/select_statement.h"

namespace peloton {
namespace parser {

const std::string InsertStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "InsertStatement\n";
  os << StringUtil::Indent(num_indent + 1) << GetTableName() << std::endl;
  if (!columns.empty()) {
    os << StringUtil::Indent(num_indent + 1) << "-> Columns\n";
    for (auto &col_name : columns) {
      os << StringUtil::Indent(num_indent + 2) << col_name << std::endl;
    }
  }
  switch (type) {
    case InsertType::VALUES:
      os << StringUtil::Indent(num_indent + 1) << "-> Values\n";
      for (auto &value_item : insert_values) {
        // TODO this is a debugging method which is currently unused.
        for (auto &expr : value_item) {
          os << expr.get()->GetInfo(num_indent + 2) << std::endl;
        }
      }
      break;
    case InsertType::SELECT:
      os << select.get()->GetInfo(num_indent + 1);
      break;
    default:
      break;
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

const std::string InsertStatement::GetInfo() const {
  std::ostringstream os;

  os << "SQLStatement[INSERT]\n";

  os << GetInfo(1);

  return os.str();
}

}  // namespace parser
}  // namespace peloton
