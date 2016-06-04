/*-------------------------------------------------------------------------
 *
 * parser.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * 
 *
 *-------------------------------------------------------------------------
 */

#include <iostream>
#include <string>

#include "parser.h"

#include "backend/parser/parser_bison.h"
#include "backend/parser/parser_flex.h"
#include "backend/common/exception.h"

namespace nstore {
namespace parser {

SQLStatementList* Parser::ParseSQLString(const char *text) {
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

} // End parser namespace
} // End nstore namespace

