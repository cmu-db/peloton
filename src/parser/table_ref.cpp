//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_ref.cpp
//
// Identification: src/parser/table_ref.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/table_ref.h"
#include "parser/select_statement.h"

namespace peloton {
namespace parser {

TableRef::~TableRef() { delete select; }

const std::string TableRef::GetInfo(int num_indent) const {
  std::ostringstream os;
  switch (type) {
    case TableReferenceType::NAME:
      os << StringUtil::Indent(num_indent) << GetTableName() << std::endl;
      break;

    case TableReferenceType::SELECT:
      os << select->GetInfo(num_indent) << std::endl;
      break;

    case TableReferenceType::JOIN:
      os << StringUtil::Indent(num_indent) << "-> Join Table\n";
      os << StringUtil::Indent(num_indent + 1) + "-> Left\n";
      os << join->left.get()->GetInfo(num_indent + 2) << std::endl;
      os << StringUtil::Indent(num_indent + 1) << "-> Right\n";
      os << join->right.get()->GetInfo(num_indent + 2) << std::endl;
      os << StringUtil::Indent(num_indent + 1) << "-> Join Condition\n";
      os << join->condition.get()->GetInfo(num_indent + 2) << std::endl;
      break;

    case TableReferenceType::CROSS_PRODUCT:
      for (auto &tbl : list) {
        os << tbl.get()->GetInfo(num_indent + 1) << std::endl;
      }
      break;

    case TableReferenceType::INVALID:
    default:
      LOG_ERROR("invalid table ref type");
      break;
  }

  if (!alias.empty()) {
    os << StringUtil::Indent(num_indent) << "Alias: " << alias;
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

}  // namespace parser
}  // namespace peloton
