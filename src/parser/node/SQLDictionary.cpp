/******************************************************************
*
* peloton for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include "parser/node/SQLSet.h"

namespace peloton {
namespace parser {

SQLSet::~SQLSet()
{
}

std::string &SQLSet::toString(std::string &buf)
{
  std::ostringstream oss;
  
  oss << name;

  SQLNode *parentNode = getParentNode();
  oss << ((isUnQLNode() && (parentNode->isColumnsNode() || parentNode->isValuesNode())) ? ":" : " = ");

  std::string valueString;
  oss << SQLExpression::toString(valueString);
  
  buf = oss.str();
  
  return buf;
}

}  // End parser namespace
}  // End peloton namespace
