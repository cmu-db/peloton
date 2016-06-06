#include<sstream>

#include "parser/SQLNode.h"
#include "parser/SQLStatement.h"
#include "parser/node/SQLExpression.h"
#include "parser/node/SQLOperator.h"

namespace peloton {
namespace parser {

const int SQLNode::COMMAND = 1;
const int SQLNode::COLUMNS = 4;
const int SQLNode::FUNCTION = 5;
const int SQLNode::CONDITION = 6;
const int SQLNode::WHERE = 7;
const int SQLNode::ORDER = 8;
const int SQLNode::ORDERBY = 9;
const int SQLNode::LIMIT = 10;
const int SQLNode::OFFSET = 11;
const int SQLNode::COLLECTION = 12;
const int SQLNode::EXPRESSION = 13;
const int SQLNode::VALUES = 15;
const int SQLNode::OPTION = 17;
const int SQLNode::OPERATOR = 18;
const int SQLNode::GROUPBY = 19;
const int SQLNode::HAVING = 20;
const int SQLNode::INDEX = 21;
const int SQLNode::TRANSACTION = 22;
const int SQLNode::SET = 25;
const int SQLNode::STATEMENT = 26;
const int SQLNode::COLLECTIONS = 27;
const int SQLNode::SETS = 28;

SQLNode::SQLNode()
{
  setParentNode(NULL);
}

SQLNode::~SQLNode()
{
}

bool SQLNode::isStatementType(int type)
{
  if ((getRootNode()->isStatementNode())) {
    SQLStatement * stmtNode = (SQLStatement *)getRootNode();
    return stmtNode->isStatementType(type);
  }

  return SQLStatement::UNKNOWN;
}

bool SQLNode::isUnQLNode()
{
  return isStatementType(SQLStatement::UNQL);
}

bool SQLNode::isExpressionNode()
{
  return (dynamic_cast<SQLExpression *>(this)) ? true : false;
}

bool SQLNode::isOperatorNode()
{
  return (dynamic_cast<SQLOperator *>(this)) ? true : false;
}

bool SQLNode::isStatementNode()
{
  return (dynamic_cast<SQLStatement *>(this)) ? true : false;
}

bool SQLNode::isDictionaryNode()
{
  return (dynamic_cast<SQLSet *>(this)) ? true : false;
}

bool SQLNode::isColumnsNode()
{
  return (dynamic_cast<SQLColumns *>(this)) ? true : false;
}

bool SQLNode::isCollectionsNode()
{
  return (dynamic_cast<SQLCollections *>(this)) ? true : false;
}

bool SQLNode::isValuesNode()
{
  return (dynamic_cast<SQLValues *>(this)) ? true : false;
}

SQLNode *SQLNode::getChildNode(size_t index)
{
  SQLNodeList *childNodes = getChildNodes();
  size_t childNodeCount = getChildCount();
  if (((childNodeCount - 1) < index))
    return NULL;
  return childNodes->at(index);
}

SQLNode *SQLNode::findChildNodeByType(int type)
{
  for (SQLNodeList::iterator node = children.begin(); node != children.end(); node++) {
    if ((*node)->isType(type))
      return (*node);
  }
  return NULL;
}

static std::string CgSQLNode2String(SQLNode *sqlNode, std::string &buf)
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

static void CgSQLNodeAdd2OutputStream(SQLNode *sqlNode, std::ostringstream &oss)
{
  std::string nodeStr;
  CgSQLNode2String(sqlNode, nodeStr);
  CgSQLNodeStringAdd2OutputStream(nodeStr, oss);
}

std::string &SQLNode::childNodesToString(std::string &buf, std::string delim)
{
  std::ostringstream oss;

  SQLNodeList *childNodes = getChildNodes();
  std::size_t numChildren = childNodes->size();
  for (size_t n=0; n<numChildren; n++) {
    std::string childNodeStr;
    SQLNode *childNode = childNodes->getNode(n);
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

std::string &SQLNode::toString(std::string &buf)
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

}  // End parser namespace
}  // End peloton namespace

