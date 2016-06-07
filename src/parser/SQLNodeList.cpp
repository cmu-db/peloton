#include<sstream>

#include "parser/SQLNode.h"
#include "parser/SQLNodes.h"

namespace peloton {
namespace parser {

static size_t SQLNodeListGetIncompleteLogicalOperatorIndex(SQLNodeList *nodeList)
{
  size_t expressionsCnt = (int)nodeList->size();
  
  // Find only 'AND' nodes.
  for (size_t n=0; n<expressionsCnt; n++) {
    SQLNode *node = nodeList->at(n);
    if (node->isOperatorNode() == false)
      continue;
    SQLOperator *operNode = (SQLOperator *)node;
    if (operNode->isAnd() == false)
      continue;
    if (operNode->getExpressions()->size() <= 0)
      return n;
  }
  
  // Find other operator nodes such as 'OR'.
  for (size_t n=0; n<expressionsCnt; n++) {
    SQLNode *node = nodeList->at(n);
    if (node->isOperatorNode() == false)
      continue;
    SQLOperator *operNode = (SQLOperator *)node;
    if (operNode->getExpressions()->size() <= 0)
      return n;
  }
  
  return -1;
}

static bool SQLNodeListAddAsChildNode(SQLNodeList *nodeList, SQLNode *parentNode, size_t childNodeIndex)
{
  if (nodeList->size() < (childNodeIndex + 1))
    return false;
    
  SQLNode *childNode = nodeList->at(childNodeIndex);
  parentNode->addChildNode(childNode);
  nodeList->erase(nodeList->begin() + childNodeIndex);
  
  return true;
}

SQLNodeList::SQLNodeList()
{
}

SQLNodeList::~SQLNodeList()
{
  clear();
}

void SQLNodeList::sort()
{
  std::string buf; 
  size_t l = size();
  if (l <= 1)
    return;
  for (SQLNodeList::iterator node = begin(); node != end(); node++) {
    //std::cout << "SORT[" << (++n) << "] : " << (*node)->toString(buf) << std::endl;
  }
  
  long long logicalOperIndex = SQLNodeListGetIncompleteLogicalOperatorIndex(this);
  while (1 <= logicalOperIndex) {
    SQLNode *logicalOperNode = at(logicalOperIndex);
    SQLNodeListAddAsChildNode(this, logicalOperNode, (logicalOperIndex - 1));
    SQLNodeListAddAsChildNode(this, logicalOperNode, (logicalOperIndex));
    logicalOperIndex = SQLNodeListGetIncompleteLogicalOperatorIndex(this);
  }
}

void SQLNodeList::clear()
{
  /* FIXME
  SQLNodeList::iterator node = begin();
  while (node != end()) {
    node = erase(node);
    delete (*node);
  }
  */
  std::vector<SQLNode *>::clear();
}


}  // End parser namespace
}  // End peloton namespace

