//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parser.cpp
//
// Identification: src/parser/parser.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>

#include "parser/parser.h"
#include "parser/sql_parser.h"
#include "parser/sql_scanner.h"
#include "common/exception.h"
#include "common/types.h"

namespace peloton {
namespace parser {

Parser::Parser(){

}

Parser::~Parser(){

}

SQLStatementList* Parser::ParseSQLString(const char* text) {
  SQLStatementList* result;
  yyscan_t scanner;
  YY_BUFFER_STATE state;

  if (yylex_init(&scanner)) {
    // couldn't initialize
    throw ParserException("Parser :: Error when initializing lexer!\n");
    return NULL;
  }

  state = yy_scan_string(text, scanner);

  if (parser_parse(&result, scanner)) {
    // Returns an error stmt object
    yy_delete_buffer(state, scanner);
    yylex_destroy(scanner);
    return result;
  }

  yy_delete_buffer(state, scanner);
  yylex_destroy(scanner);
  return result;
}

SQLStatementList* Parser::ParseSQLString(const std::string& text) {
  return ParseSQLString(text.c_str());
}

Parser &Parser::GetInstance(){
  static Parser parser;
  return parser;
}

std::unique_ptr<parser::SQLStatementList> Parser::BuildParseTree(const std::string& query_string){
  auto stmt  = Parser::ParseSQLString(query_string);

  LOG_TRACE("Number of statements: %lu" ,stmt->GetStatements().size());
  
  std::unique_ptr<parser::SQLStatementList> sql_stmt (stmt);
  return std::move(sql_stmt);
}



}  // End parser namespace
}  // End peloton namespace
