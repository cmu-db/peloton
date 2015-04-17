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

namespace nstore {
namespace parser {

std::ostream& operator<<(std::ostream& os, const SQLStatement& stmt) {

  os << "\tSTMT : Type :: " << stmt._type << "\n";

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


