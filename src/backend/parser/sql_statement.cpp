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

#include "sql_statement.h"

#include "backend/parser/parser_utils.h"

namespace nstore {
namespace parser {

std::ostream& operator<<(std::ostream& os, const SQLStatement& stmt) {

  os << "STATEMENT : Type :: " << stmt.stmt_type << "\n";

  int indent = 1;

  switch(stmt.stmt_type) {
    case STATEMENT_TYPE_SELECT:
      GetSelectStatementInfo((SelectStatement*)&stmt, indent);
      break;
    case STATEMENT_TYPE_INSERT:
      GetInsertStatementInfo((InsertStatement*)&stmt, indent);
      break;
    case STATEMENT_TYPE_CREATE:
      GetCreateStatementInfo((CreateStatement*)&stmt, indent);
      break;
    default:
      break;
  }

  return os;
}

std::ostream& operator<<(std::ostream& os, const SQLStatementList& stmt_list) {

  if(stmt_list.is_valid) {
    for(auto stmt : stmt_list.statements)
      os << *stmt;
  }
  else {
      os << "Invalid statement list \n";
  }

  return os;
}


} // End parser namespace
} // End nstore namespace


