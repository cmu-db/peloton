/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include <sstream>

#include "parser/SQLStatement.h"

const int uSQL::SQLStatement::UNKNOWN = -1;
const int uSQL::SQLStatement::SQL92 = 1;
const int uSQL::SQLStatement::GQL = 2;
const int uSQL::SQLStatement::UNQL = 2;

uSQL::SQLStatement::SQLStatement()
{
  setType(STATEMENT);
  setStatementType(UNKNOWN);
}

std::string &uSQL::SQLStatement::toString(std::string &buf)
{
  return SQLNode::toString(buf);
}

static void CgSQLStatementPrintTree(std::ostringstream &oss, uSQL::SQLNode *node, size_t indent)
{
  for (size_t n=0; n<indent; n++)
    oss << "| ";
  std::string buf;
  oss << "|-- " << node->toString(buf) << " ("<< node->getTypeName() << ")" << std::endl;
  
  uSQL::SQLNodeList *childNodes = node->getChildNodes();
  std::size_t numChildren = childNodes->size();
  for (size_t n=0; n<numChildren; n++)
    CgSQLStatementPrintTree(oss, childNodes->getNode(n), (indent+1));
}

std::string &uSQL::SQLStatement::toTreeString(std::string &buf)
{
  std::ostringstream oss;
  
  uSQL::SQLNodeList *childNodes = getChildNodes();
  std::size_t numChildren = childNodes->size();
  for (size_t n=0; n<numChildren; n++)
    CgSQLStatementPrintTree(oss, childNodes->getNode(n), 0);

  buf = oss.str();
  
  return buf;
}
