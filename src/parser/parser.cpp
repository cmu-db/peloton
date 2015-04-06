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
  GdaStatement *stmt = NULL;
  const gchar *remain;
  GError *error = NULL;

  // create parser
  parser = gda_sql_parser_new();

  stmt = gda_sql_parser_parse_string(parser, query.c_str(), &remain, &error);

  //Serialize to json
  std::cout << gda_statement_serialize(stmt) << "\n";

}

} // End parser namespace
} // End nstore namespace

