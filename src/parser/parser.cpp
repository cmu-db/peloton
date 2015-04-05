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
#include "sql-parser/gda-sql-parser.h"

namespace nstore {
namespace parser {

void Parser::ParseSQLString(std::string query) {

  GdaSqlParser *parser = NULL;
  GdaStatement *statement = NULL;

  /* Do parse SQL */
  parser = gda_sql_parser_new();
  statement = gda_sql_parser_parse_string(parser, query.c_str(), NULL, NULL);

  //Serialize to json
  std::cout << gda_statement_serialize(statement) << "\n";

  delete parser;
  delete statement;
}

} // End parser namespace
} // End nstore namespace

