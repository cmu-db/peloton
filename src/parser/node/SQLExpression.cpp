/******************************************************************
*
* peloton for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include "parser/node/SQLExpression.h"

namespace peloton {
namespace parser {

const int SQLExpression::UNKOWN = -1;
const int SQLExpression::STRING = 1;
const int SQLExpression::PROPERTY = 2;
const int SQLExpression::INTEGER = 3;
const int SQLExpression::REAL = 4;
const int SQLExpression::BOOLEAN = 5;
const int SQLExpression::OPERATOR = 6;
const int SQLExpression::FUNCTION = 7;
const int SQLExpression::SELECT = 8;
const int SQLExpression::BLOB = 9;
const int SQLExpression::NIL = 10;
const int SQLExpression::CURRENT_TIME = 11;
const int SQLExpression::CURRENT_DATE = 12;
const int SQLExpression::CURRENT_TIMESTAMP = 13;
const int SQLExpression::ASTERISK = 14;

void SQLExpression::setLiteralType(int type)
{
  this->literalType = type;
  
  switch (this->literalType) {
  case NIL:
    setValue("NULL");
    break;
  }
}

void SQLExpression::set(SQLExpression *exprNode)
{
  std::string nodeValue = exprNode->getValue();
  setValue(nodeValue);
}

std::string &SQLExpression::toExpressionString(std::string &buf)
{
  std::ostringstream oss;
  
  bool isUnQL = isUnQLNode();
  
  SQLNodeList *expressions = getChildNodes();
  std::size_t expressionsCount = expressions->size();

  bool hasDictionaryValues = false;
  for (size_t n=0; n<expressionsCount; n++) {
    SQLNode *sqlNode = expressions->getNode(n);
    if (sqlNode->isExpressionNode() == false)
      continue;
    SQLExpression *exprNode = (SQLExpression *)sqlNode;
    if (exprNode->isDictionaryNode() == true) {
      hasDictionaryValues = true;
      break;
    }
  }
  
  bool isAsterisk = false;
  if (1 == expressionsCount) {
    SQLNode *sqlNode = expressions->getNode(0);
    if (sqlNode->isExpressionNode() == true) {
      SQLExpression *exprNode = (SQLExpression *)sqlNode;
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

}  // End parser namespace
}  // End peloton namespace

