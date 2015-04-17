/*-------------------------------------------------------------------------
 *
 * parser.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/parser/parser.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <iostream>
#include <string>

#include "parser/parser.h"
#include "parser_bison.h"
#include "parser_flex.h"

namespace nstore {
namespace parser {

Parser::Parser() {
  fprintf(stderr, "Parser only has static methods atm! Do not initialize!\n");
}

SQLStatementList* Parser::ParseSQLString(const char *text) {
  SQLStatementList* result;
  yyscan_t scanner;
  YY_BUFFER_STATE state;

  if (yylex_init(&scanner)) {
    // couldn't initialize
    fprintf(stderr, "[Error] Parser: Error when initializing lexer!\n");
    return NULL;
  }

  state = yy_scan_string(text, scanner);

  if (parser_parse(&result, scanner)) {
    // Returns an error stmt object
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

