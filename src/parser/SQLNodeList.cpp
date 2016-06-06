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
#include "parser/SQLNodes.h"

static size_t SQLNodeListGetIncompleteLogicalOperatorIndex(uSQL::SQLNodeList *nodeList)
{
  size_t expressionsCnt = (int)nodeList->size();
  
  // Find only 'AND' nodes.
  for (size_t n=0; n<expressionsCnt; n++) {
    uSQL::SQLNode *node = nodeList->at(n);
    if (node->isOperatorNode() == false)
      continue;
    uSQL::SQLOperator *operNode = (uSQL::SQLOperator *)node;
    if (operNode->isAnd() == false)
      continue;
    if (operNode->getExpressions()->size() <= 0)
      return n;
  }
  
  // Find other operator nodes such as 'OR'.
  for (size_t n=0; n<expressionsCnt; n++) {
    uSQL::SQLNode *node = nodeList->at(n);
    if (node->isOperatorNode() == false)
      continue;
    uSQL::SQLOperator *operNode = (uSQL::SQLOperator *)node;
    if (operNode->getExpressions()->size() <= 0)
      return n;
  }
  
  return -1;
}

static bool SQLNodeListAddAsChildNode(uSQL::SQLNodeList *nodeList, uSQL::SQLNode *parentNode, size_t childNodeIndex)
{
  if (nodeList->size() < (childNodeIndex + 1))
    return false;
    
  uSQL::SQLNode *childNode = nodeList->at(childNodeIndex);
  parentNode->addChildNode(childNode);
  nodeList->erase(nodeList->begin() + childNodeIndex);
  
  return true;
}

uSQL::SQLNodeList::SQLNodeList()
{
}

uSQL::SQLNodeList::~SQLNodeList()
{
  clear();
}

void uSQL::SQLNodeList::sort()
{
  std::string buf; 
  size_t l = size();
  if (l <= 1)
    return;
  for (uSQL::SQLNodeList::iterator node = begin(); node != end(); node++) {
    //std::cout << "SORT[" << (++n) << "] : " << (*node)->toString(buf) << std::endl;
  }
  
  size_t logicalOperIndex = SQLNodeListGetIncompleteLogicalOperatorIndex(this);
  while (1 <= logicalOperIndex) {
    uSQL::SQLNode *logicalOperNode = at(logicalOperIndex);
    SQLNodeListAddAsChildNode(this, logicalOperNode, (logicalOperIndex - 1));
    SQLNodeListAddAsChildNode(this, logicalOperNode, (logicalOperIndex));
    logicalOperIndex = SQLNodeListGetIncompleteLogicalOperatorIndex(this);
  }
}

void uSQL::SQLNodeList::clear()
{
  /* FIXME
  uSQL::SQLNodeList::iterator node = begin();
  while (node != end()) {
    node = erase(node);
    delete (*node);
  }
  */
  std::vector<SQLNode *>::clear();
}
