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

#include "parser_utils.h"

namespace nstore {
namespace parser {

std::ostream& operator<<(std::ostream& os, const SQLStatement& stmt) {

  os << "\tSTMT : Type :: " << stmt._type << "\n";

  int indent = 1;

  switch(stmt._type) {
    case kStmtSelect:
      printSelectStatementInfo((SelectStatement*)&stmt, indent);
      break;
    case kStmtImport:
      printImportStatementInfo((ImportStatement*)&stmt, indent);
      break;
    case kStmtInsert:
      printInsertStatementInfo((InsertStatement*)&stmt, indent);
      break;
    case kStmtCreate:
      printCreateStatementInfo((CreateStatement*)&stmt, indent);
      break;
    default:
      break;
  }

  return os;
}

std::ostream& operator<<(std::ostream& os, const SQLStatementList& stmt_list) {

  if(stmt_list.isValid) {
    for(auto stmt : stmt_list.statements)
      os << *stmt;
  }
  else {
      os << "Invalid statment list \n";
  }

  return os;
}


} // End parser namespace
} // End nstore namespace


