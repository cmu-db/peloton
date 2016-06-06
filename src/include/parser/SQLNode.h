/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLNODE_H_
#define _USQL_SQLNODE_H_

#include <vector>
#include <iostream>
#include <sstream>

namespace uSQL {

class SQLNode;
class SQLStatement;
  
class SQLNodeList : public std::vector<SQLNode *> {

public:

  SQLNodeList();
  ~SQLNodeList();
  
  void addNode(SQLNode *node) {
    push_back(node);
  }

  SQLNode *getNode(std::size_t index) {
    if ((size()-1) < index)
      return NULL;
    return at(index);
  }
  
  void sort();
  
  void clear();
};

class SQLNode {

public:

  static const int COLLECTION;
  static const int COLLECTIONS;
  static const int CONDITION;
  static const int COLUMNS;
  static const int COMMAND;
  static const int EXPRESSION;
  static const int FUNCTION;
  static const int GROUPBY;
  static const int HAVING;
  static const int INDEX;
  static const int LIMIT;
  static const int OFFSET;
  static const int OPERATOR;
  static const int OPTION;
  static const int ORDER;
  static const int ORDERBY;
  static const int SET;
  static const int SETS;
  static const int STATEMENT;
  static const int TRANSACTION;
  static const int WHERE;
  static const int VALUES;
  
private:
  
  int type;

  SQLNode *parent;
  SQLNodeList children;

  std::string value;
  
  std::string nodeString;

private:

  bool isStatementType(int type);
  
public:

  SQLNode();

  virtual ~SQLNode();
  
  void setType(int type) {
    this->type = type;
  }
  
  int getType() {
    return this->type;
  }
  
  bool isType(int type) {
    return (this->type == type) ? true : false;
  }
  
  virtual const char *getTypeName() = 0;
  
  void setValue(const std::string &value) {
    this->value = value;
  }
  
  const std::string &getValue() {
    return this->value;
  }

  bool equals(const std::string &value) {
    return (this->value.compare(value) == 0) ? true : false;
  }
  
  void setParentNode(SQLNode *node) {
    this->parent = node;
  }
  
  SQLNode *getParentNode() {
    return this->parent;
  }

  SQLNode *getRootNode() {
    SQLNode *rootNode = getParentNode();
    while (rootNode && rootNode->getParentNode())
      rootNode = rootNode->getParentNode();
    return rootNode;
  }
  
  void addChildNode(SQLNode *node) {
    node->setParentNode(this);
    children.addNode(node);
  }
    
  void addChildNodes(SQLNodeList *nodeList) {
    for (SQLNodeList::iterator node = nodeList->begin(); node != nodeList->end(); node++)
      addChildNode(*node);
  }

  bool hasChildNodes()
  {
    return (0 < children.size()) ? true : false;
  }
  
  int getChildCount() {
    return (int)children.size();
  }
  
  SQLNodeList *getChildNodes()
  {
    return &children;
  }

  SQLNode *getChildNode(size_t index);  
  SQLNode *findChildNodeByType(int type);

  void clearChildNodes()
  {
    children.clear();
  }
  
  bool isExpressionNode();
  bool isOperatorNode();
  bool isStatementNode();
  bool isDictionaryNode();
  bool isColumnsNode();
  bool isCollectionsNode();
  bool isValuesNode();
  
  bool isUnQLNode();

  virtual std::string &toString(std::string &buf);
  
  const std::string &toString() {
    return toString(this->nodeString);
  }
  
protected:

  std::string &childNodesToString(std::string &buf, std::string delim = " ");
};

}

#endif
