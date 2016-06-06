/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include "parser/node/SQLSet.h"

uSQL::SQLSet::~SQLSet()
{
}

std::string &uSQL::SQLSet::toString(std::string &buf) 
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
