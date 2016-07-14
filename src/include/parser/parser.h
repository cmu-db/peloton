//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parser.h
//
// Identification: src/include/parser/parser.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/statements.h"

namespace peloton {
namespace parser {

//===--------------------------------------------------------------------===//
// Parser
//===--------------------------------------------------------------------===//

class Parser {
  
  Parser();
  ~Parser();
 
 public:
  // Parse a given query
  static SQLStatementList* ParseSQLString(const char* sql);
  static SQLStatementList* ParseSQLString(const std::string& sql);

  static Parser &GetInstance();

  std::unique_ptr<parser::SQLStatement> BuildParseTree(const std::string& query_string);
};

}  // End parser namespace
}  // End peloton namespace
