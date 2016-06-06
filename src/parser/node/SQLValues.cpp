/******************************************************************
*
* peloton for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include "parser/SQLStatement.h"
#include "parser/node/SQLValues.h"

namespace peloton {
namespace parser {

std::string &SQLValues::toString(std::string &buf)
{
  bool isUnQL = false;
  if ((getRootNode()->isStatementNode())) {
    SQLStatement *stmtNode = (SQLStatement *)getRootNode();
    isUnQL = stmtNode->isUnQL();
  }
  
  std::string columnString;
  std::ostringstream oss;
  oss << (isUnQL ? "VALUE" : "VALUES") << " " << toExpressionString(columnString);
  buf = oss.str();
  return buf;
}

}  // End parser namespace
}  // End peloton namespace
