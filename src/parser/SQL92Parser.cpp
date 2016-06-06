/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include <antlr3.h>

#include "parser/SQL92Parser.h"

uSQL::SQL92Parser::SQL92Parser()
{
}

bool uSQL::SQL92Parser::parse(const std::string &queryString)
{
  bool parseResult = SQLParser::parse(queryString);
  setStatementType(uSQL::SQLStatement::SQL92);
  return parseResult;
}
