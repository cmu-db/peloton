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
#include <sql-parser/gda-sql-parser.h>

namespace nstore {
namespace parser {

void Parser::ParseSQLString(std::string query) {

  GdaSqlParser *pParser = NULL;
  GdaStatement *pStatement = NULL;
  GError *pError = NULL;
  GdaSqlStatement *pSqlStatement = NULL;

  /* Do parse SQL */
  pParser = gda_sql_parser_new();
  pStatement = gda_sql_parser_parse_string(pParser, query.c_str(), NULL, NULL);

  //Serialize to json
  printf("%s",gda_statement_serialize(pStatement));
}

} // End parser namespace
} // End nstore namespace

