/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include<sstream>

#include "parser/SQLStatement.h"

uSQL::SQLStatementList::SQLStatementList()
{
}

uSQL::SQLStatementList::~SQLStatementList()
{
  clear();
}

void uSQL::SQLStatementList::clear()
{
  /* FIXME
  uSQL::SQLStatementList::iterator stmt = begin();
  while (stmt != end()) {
    stmt = erase(stmt);
    delete (*stmt);
  }
  */
  std::vector<SQLStatement *>::clear();
}
