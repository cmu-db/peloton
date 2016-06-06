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

#include "parser/SQLNode.h"
#include "parser/SQLStatement.h"
#include "parser/node/SQLExpression.h"
#include "parser/node/SQLOperator.h"

const int uSQL::SQLNode::COMMAND = 1;
const int uSQL::SQLNode::COLUMNS = 4;
const int uSQL::SQLNode::FUNCTION = 5;
const int uSQL::SQLNode::CONDITION = 6;
const int uSQL::SQLNode::WHERE = 7;
const int uSQL::SQLNode::ORDER = 8;
const int uSQL::SQLNode::ORDERBY = 9;
const int uSQL::SQLNode::LIMIT = 10;
const int uSQL::SQLNode::OFFSET = 11;
const int uSQL::SQLNode::COLLECTION = 12;
const int uSQL::SQLNode::EXPRESSION = 13;
const int uSQL::SQLNode::VALUES = 15;
const int uSQL::SQLNode::OPTION = 17;
const int uSQL::SQLNode::OPERATOR = 18;
const int uSQL::SQLNode::GROUPBY = 19;
const int uSQL::SQLNode::HAVING = 20;
const int uSQL::SQLNode::INDEX = 21;
const int uSQL::SQLNode::TRANSACTION = 22;
const int uSQL::SQLNode::SET = 25;
const int uSQL::SQLNode::STATEMENT = 26;
const int uSQL::SQLNode::COLLECTIONS = 27;
const int uSQL::SQLNode::SETS = 28;

uSQL::SQLNode::SQLNode()
{
  setParentNode(NULL);
}

uSQL::SQLNode::~SQLNode()
{
}

bool uSQL::SQLNode::isStatementType(int type)
{
  if ((getRootNode()->isStatementNode())) {
    uSQL::SQLStatement * stmtNode = (uSQL::SQLStatement *)getRootNode();
    return stmtNode->isStatementType(type);
  }

  return uSQL::SQLStatement::UNKNOWN;
}

bool uSQL::SQLNode::isUnQLNode()
{
  return isStatementType(uSQL::SQLStatement::UNQL);
}

bool uSQL::SQLNode::isExpressionNode() 
{
  return (dynamic_cast<uSQL::SQLExpression *>(this)) ? true : false;
}

bool uSQL::SQLNode::isOperatorNode() 
{
  return (dynamic_cast<uSQL::SQLOperator *>(this)) ? true : false;
}

bool uSQL::SQLNode::isStatementNode() 
{
  return (dynamic_cast<uSQL::SQLStatement *>(this)) ? true : false;
}

bool uSQL::SQLNode::isDictionaryNode() 
{
  return (dynamic_cast<uSQL::SQLSet *>(this)) ? true : false;
}

bool uSQL::SQLNode::isColumnsNode() 
{
  return (dynamic_cast<uSQL::SQLColumns *>(this)) ? true : false;
}

bool uSQL::SQLNode::isCollectionsNode() 
{
  return (dynamic_cast<uSQL::SQLCollections *>(this)) ? true : false;
}

bool uSQL::SQLNode::isValuesNode() 
{
  return (dynamic_cast<uSQL::SQLValues *>(this)) ? true : false;
}

uSQL::SQLNode *uSQL::SQLNode::getChildNode(size_t index) 
{
  uSQL::SQLNodeList *childNodes = getChildNodes();
  size_t childNodeCount = getChildCount();
  if (((childNodeCount - 1) < index))
    return NULL;
  return childNodes->at(index);
}

uSQL::SQLNode *uSQL::SQLNode::findChildNodeByType(int type)
{
  for (SQLNodeList::iterator node = children.begin(); node != children.end(); node++) {
    if ((*node)->isType(type))
      return (*node);
  }
  return NULL;
}

static std::string CgSQLNode2String(uSQL::SQLNode *sqlNode, std::string &buf)
{
  std::ostringstream oss;
  
  const std::string value = sqlNode->getValue();
  if (0 < value.length()) {
    oss << value;
  }
  
  buf = oss.str();
  
  return buf;
}

static void CgSQLNodeStringAdd2OutputStream(std::string &nodeString, std::ostringstream &oss)
{
  if (nodeString.length() <= 0)
    return;
    
  std::string ossStr = oss.str();
  if (0 < ossStr.length()) {
    char lastChar = ossStr.at((ossStr.length()-1));
    if (lastChar != ' ')
      oss << " ";
  }
  
  oss << nodeString;
}

static void CgSQLNodeAdd2OutputStream(uSQL::SQLNode *sqlNode, std::ostringstream &oss)
{
  std::string nodeStr;
  CgSQLNode2String(sqlNode, nodeStr);
  CgSQLNodeStringAdd2OutputStream(nodeStr, oss);
}

std::string &uSQL::SQLNode::childNodesToString(std::string &buf, std::string delim)
{
  std::ostringstream oss;

  uSQL::SQLNodeList *childNodes = getChildNodes();
  std::size_t numChildren = childNodes->size();
  for (size_t n=0; n<numChildren; n++) {
    std::string childNodeStr;
    uSQL::SQLNode *childNode = childNodes->getNode(n);
    childNode->toString(childNodeStr);
    if (0 < childNodeStr.length()) {
      oss << childNodeStr;
      if (n < (numChildren-1))
        oss << delim;
    }
  }
  
  buf = oss.str();
  
  return buf;
}

std::string &uSQL::SQLNode::toString(std::string &buf)
{
  std::ostringstream oss;

  CgSQLNodeAdd2OutputStream(this, oss);

  if (hasChildNodes() == true) {
    std::string childNodeStr;
    childNodesToString(childNodeStr);
    CgSQLNodeStringAdd2OutputStream(childNodeStr, oss);
  }
  
  buf = oss.str();
  
  return buf;
}
