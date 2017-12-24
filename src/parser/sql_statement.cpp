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
#include "parser/parser_utils.h"

namespace peloton {
namespace parser {

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

const std::string SQLStatement::GetInfo() const {
  std::ostringstream os;

  os << "SQLStatement[" << StatementTypeToString(stmt_type) << "]\n";

  int indent = 1;
  switch (stmt_type) {
    case StatementType::SELECT:
      os << ParserUtils::GetSelectStatementInfo((SelectStatement*)this, indent);
      break;
    case StatementType::INSERT:
      os << ParserUtils::GetInsertStatementInfo((InsertStatement*)this, indent);
      break;
    case StatementType::CREATE:
      os << ParserUtils::GetCreateStatementInfo((CreateStatement*)this, indent);
      break;
    case StatementType::DELETE:
      os << ParserUtils::GetDeleteStatementInfo((DeleteStatement*)this, indent);
      break;
    case StatementType::COPY:
      os << ParserUtils::GetCopyStatementInfo((CopyStatement*)this, indent);
      break;
    case StatementType::UPDATE:
      os << ParserUtils::GetUpdateStatementInfo((UpdateStatement*)this, indent);
      break;
    default:
      break;
  }
  return os.str();
}

const std::string SQLStatementList::GetInfo() const {
  std::ostringstream os;

  if (is_valid) {
    for (auto& stmt : statements) {
      os << stmt->GetInfo() << std::endl;
    }
  } else {
    os << "Invalid statement list";
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

}  // namespace parser
}  // namespace peloton
