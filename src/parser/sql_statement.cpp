//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_statement.cpp
//
// Identification: src/parser/sql_statement.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/*-------------------------------------------------------------------------
 *
 * sql_statement.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/parser/sql_statement.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <sstream>

#include "parser/sql_statement.h"
#include "util/string_util.h"

namespace peloton {
namespace parser {

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

const std::string SQLStatement::GetInfo(int num_indent) const {
  std::ostringstream os;
  os << StringUtil::Indent(num_indent) << "GetInfo for statement type "
     << StatementTypeToString(stmt_type) << " not implemented yet...\n";
  return os.str();
}

const std::string SQLStatement::GetInfo() const {
  std::ostringstream os;

  os << "SQLStatement[" << StatementTypeToString(stmt_type) << "]\n";

  int indent = 1;
  os << GetInfo(indent) << std::endl;
  return os.str();
}

const std::string SQLStatementList::GetInfo(int num_indent) const {
  std::ostringstream os;

  if (is_valid) {
    for (auto &stmt : statements) {
      os << stmt->GetInfo(num_indent) << std::endl;
    }
  } else {
    os << "Invalid statement list";
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

const std::string SQLStatementList::GetInfo() const {
  std::ostringstream os;

  os << GetInfo(0);

  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

}  // namespace parser
}  // namespace peloton
