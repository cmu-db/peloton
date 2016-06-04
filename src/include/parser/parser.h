/*-------------------------------------------------------------------------
 *
 * parser.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * 
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "parser/statements.h"

namespace peloton {
namespace parser {

//===--------------------------------------------------------------------===//
// Parser
//===--------------------------------------------------------------------===//

class Parser {
  Parser() = delete;

 public:

  // Parse a given query
  static SQLStatementList* ParseSQLString(const char* sql);
  static SQLStatementList* ParseSQLString(const std::string& sql);

};


} // End parser namespace
} // End peloton namespace
