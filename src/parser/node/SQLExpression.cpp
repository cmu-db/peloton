/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include "parser/node/SQLExpression.h"

const int uSQL::SQLExpression::UNKOWN = -1;
const int uSQL::SQLExpression::STRING = 1;
const int uSQL::SQLExpression::PROPERTY = 2;
const int uSQL::SQLExpression::INTEGER = 3;
const int uSQL::SQLExpression::REAL = 4;
const int uSQL::SQLExpression::BOOLEAN = 5;
const int uSQL::SQLExpression::OPERATOR = 6;
const int uSQL::SQLExpression::FUNCTION = 7;
const int uSQL::SQLExpression::SELECT = 8;
const int uSQL::SQLExpression::BLOB = 9;
const int uSQL::SQLExpression::NIL = 10;
const int uSQL::SQLExpression::CURRENT_TIME = 11;
const int uSQL::SQLExpression::CURRENT_DATE = 12;
const int uSQL::SQLExpression::CURRENT_TIMESTAMP = 13;
const int uSQL::SQLExpression::ASTERISK = 14;

void uSQL::SQLExpression::setLiteralType(int type)
{
  this->literalType = type;
  
  switch (this->literalType) {
  case NIL:
    setValue("NULL");
    break;
  }
}

void uSQL::SQLExpression::set(uSQL::SQLExpression *exprNode)
{
  std::string nodeValue = exprNode->getValue();
  setValue(nodeValue);
}

std::string &uSQL::SQLExpression::toExpressionString(std::string &buf) 
{
  std::ostringstream oss;
  
  bool isUnQL = isUnQLNode();
  
  uSQL::SQLNodeList *expressions = getChildNodes();
  std::size_t expressionsCount = expressions->size();

  bool hasDictionaryValues = false;
  for (size_t n=0; n<expressionsCount; n++) {
    SQLNode *sqlNode = expressions->getNode(n);
    if (sqlNode->isExpressionNode() == false)
      continue;
    uSQL::SQLExpression *exprNode = (uSQL::SQLExpression *)sqlNode;
    if (exprNode->isDictionaryNode() == true) {
      hasDictionaryValues = true;
      break;
    }
  }
  
  bool isAsterisk = false;
  if (1 == expressionsCount) {
    SQLNode *sqlNode = expressions->getNode(0);
    if (sqlNode->isExpressionNode() == true) {
      uSQL::SQLExpression *exprNode = (uSQL::SQLExpression *)sqlNode;
      isAsterisk = exprNode->isAsterisk();
    }
  }
  
  if (isUnQL) {
    if (1 < expressionsCount) {
      if (isAsterisk == false)
        oss << (hasDictionaryValues ? "{" : "[");
    }
    else {
      if (hasDictionaryValues == true)
        oss << "{";
    }
  } else {
    if (isAsterisk == false)
      oss << "(";
  }
    
  std::string childNodeStr;
  oss << childNodesToString(childNodeStr, ",");
  
  
  if (isUnQL) {
    if (1 < expressionsCount) {
      if (isAsterisk == false)
        oss << (hasDictionaryValues ? "}" : "]");
    }
    else {
      if (hasDictionaryValues == true)
        oss << "}";
    }
  } else {
    if (isAsterisk == false)
      oss << ")";
  }
  
  buf = oss.str();
  
  return buf;
}
