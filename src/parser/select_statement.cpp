//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// select_statement.cpp
//
// Identification: src/parser/select_statement.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/select_statement.h"

namespace peloton {
namespace parser {

const std::string SelectStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "SelectStatement\n";
  os << StringUtil::Indent(num_indent + 1) << "-> Fields:\n";
  for (auto &expr : select_list) {
    os << expr.get()->GetInfo(num_indent + 2) << std::endl;
  }

  if (from_table != NULL) {
    os << StringUtil::Indent(num_indent + 1) + "-> Sources:\n";
    os << from_table.get()->GetInfo(num_indent + 2) << std::endl;
  }

  if (where_clause != NULL) {
    os << StringUtil::Indent(num_indent + 1) << "-> Search Conditions:\n";
    os << where_clause.get()->GetInfo(num_indent + 1) << std::endl;
  }

  if (union_select != NULL) {
    os << StringUtil::Indent(num_indent + 1) << "-> Union:\n";
    os << union_select.get()->GetInfo(num_indent + 2) << std::endl;
  }

  if (order != NULL) {
    os << StringUtil::Indent(num_indent + 1) << "-> OrderBy:\n";
    for (size_t idx = 0; idx < order->exprs.size(); idx++) {
      auto &expr = order->exprs.at(idx);
      auto &type = order->types.at(idx);
      os << expr.get()->GetInfo(num_indent + 2) << std::endl;
      if (type == kOrderAsc)
        os << StringUtil::Indent(num_indent + 2) << "ascending\n";
      else
        os << StringUtil::Indent(num_indent + 2) << "descending\n";
    }
  }

  if (group_by != NULL) {
    os << StringUtil::Indent(num_indent + 1) << "-> GroupBy:\n";
    for (auto &column : group_by->columns) {
      os << column->GetInfo(num_indent + 2) << std::endl;
    }
    if (group_by->having) {
      os << group_by->having->GetInfo(num_indent + 2) << std::endl;
    }
  }

  if (limit != NULL) {
    os << StringUtil::Indent(num_indent + 1) << "-> Limit:\n";
    os << StringUtil::Indent(num_indent + 2)
       << "limit: " << std::to_string(limit->limit) << std::endl;
    os << StringUtil::Indent(num_indent + 2)
       << "offset: " << std::to_string(limit->offset) << std::endl;
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

const std::string SelectStatement::GetInfo() const {
  std::ostringstream os;

  os << "SQLStatement[SELECT]\n";

  os << GetInfo(1);

  return os.str();
}

}  // namespace parser
}  // namespace peloton
