/******************************************************************
*
* peloton for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include <sstream>

#include "parser/SQLStatement.h"

namespace peloton {
namespace parser {

const int SQLStatement::UNKNOWN = -1;
const int SQLStatement::SQL92 = 1;
const int SQLStatement::GQL = 2;
const int SQLStatement::UNQL = 2;

SQLStatement::SQLStatement()
{
  setType(STATEMENT);
  setStatementType(UNKNOWN);
}

std::string &SQLStatement::toString(std::string &buf)
{
  return SQLNode::toString(buf);
}

static void CgSQLStatementPrintTree(std::ostringstream &oss, SQLNode *node, size_t indent)
{
  for (size_t n=0; n<indent; n++)
    oss << "| ";
  std::string buf;
  oss << "|-- " << node->toString(buf) << " ("<< node->getTypeName() << ")" << std::endl;
  
  SQLNodeList *childNodes = node->getChildNodes();
  std::size_t numChildren = childNodes->size();
  for (size_t n=0; n<numChildren; n++)
    CgSQLStatementPrintTree(oss, childNodes->getNode(n), (indent+1));
}

std::string &SQLStatement::toTreeString(std::string &buf)
{
  std::ostringstream oss;
  
  SQLNodeList *childNodes = getChildNodes();
  std::size_t numChildren = childNodes->size();
  for (size_t n=0; n<numChildren; n++)
    CgSQLStatementPrintTree(oss, childNodes->getNode(n), 0);

  buf = oss.str();
  
  return buf;
}

}  // End parser namespace
}  // End peloton namespace
