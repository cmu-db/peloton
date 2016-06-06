/******************************************************************
*
* peloton for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#include<sstream>

#include "parser/node/SQLOperator.h"

namespace peloton {
namespace parser {

const int SQLOperator::UNKNOWN = 0;
const int SQLOperator::SEQ = 1;
const int SQLOperator::DEQ = 2;
const int SQLOperator::LT = 3;
const int SQLOperator::LE = 4;
const int SQLOperator::GT = 5;
const int SQLOperator::GE = 6;
const int SQLOperator::NOTEQ = 7;
const int SQLOperator::AND = 8;
const int SQLOperator::OR = 9;

std::string &SQLOperator::toString(std::string &buf)
{
  SQLExpression *firstExpr = getLeftExpression();

  std::ostringstream oss;
  std::string exprBuf;
      
  if (firstExpr)
    oss << firstExpr->toString(exprBuf) << " ";
    
  std::string operStr;
  
  switch (getValue()) {
  case SEQ:
    operStr = "=";
    break;
  case DEQ:
    operStr = "==";
    break;
  case NOTEQ:
    operStr = "!=";
    break;
  case LT:
    operStr = "<";
    break;
  case LE:
    operStr = "<=";
    break;
  case GT:
    operStr = ">";
    break;
  case GE:
    operStr = ">=";
    break;
  case AND:
    operStr = "AND";
    break;
  case OR:
    operStr = "OR";
    break;
  default:
    operStr = "?";
    break;
  }
  
  SQLNodeList *expressions = getExpressions();
  std::size_t expressionCnt = expressions->size();
  for (std::size_t n=1; n <expressionCnt; n++) {
    SQLNode *exprNode = expressions->at(n);
    oss << operStr << " " << exprNode->toString(exprBuf);
  }
  
  buf = oss.str();
  
  return buf;
}

}  // End parser namespace
}  // End peloton namespace

