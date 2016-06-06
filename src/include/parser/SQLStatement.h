#pragma once

#include "parser/SQLNode.h"
#include "parser/SQLNodes.h"

namespace peloton {
namespace parser {

class SQLStatement;

class SQLStatementList : public std::vector<SQLStatement *> {

public:

  SQLStatementList();
  ~SQLStatementList();

  void addStatement(SQLStatement *stmt) {
    push_back(stmt);
  }

  SQLStatement *getStatement(size_t index) {
    return at(index);
  }

  void clear();
};

class SQLStatement : public SQLNode {

public:
  
  static const int UNKNOWN;
  static const int SQL92;
  static const int GQL;
  static const int UNQL;
  
private:
  
  int statementType;

public:
  
  SQLStatement();

  const char *getTypeName() {
    return "SQLStatement";
  }
  
  void setStatementType(int type) {
    statementType = type;
  }
  
  int getStatementType(int) {
    return this->statementType;
  }
  
  bool isStatementType(int type) {
    return (this->statementType == type);
  }
  
  bool isSQL92() {
    return isStatementType(SQL92);
  }
  
  bool isGQL() {
    return isStatementType(GQL);
  }
  
  bool isUnQL() {
    return isStatementType(UNQL);
  }
  
  SQLCommand *getCommandNode() {
    return (SQLCommand *)findChildNodeByType(SQLNode::COMMAND);
  }

  SQLCollections *getCollectionsNode() {
    return (SQLCollections *)findChildNodeByType(SQLNode::COLLECTIONS);
  }
  
  SQLCollection *getCollectionNode() {
    SQLCollections *collectionsNode = getCollectionsNode();
    if (!collectionsNode)
      return NULL;
    return (SQLCollection *)collectionsNode->getCollectionNode(0);
  }
  
  SQLSets *getSetsNode() {
    return (SQLSets *)findChildNodeByType(SQLNode::SETS);
  }
  
  SQLColumns *getColumnsNode() {
    return (SQLColumns *)findChildNodeByType(SQLNode::COLUMNS);
  }

  SQLValues *getValuesNode() {
    return (SQLValues *)findChildNodeByType(SQLNode::VALUES);
  }
  
  SQLWhere *getWhereNode() {
    return (SQLWhere *)findChildNodeByType(SQLNode::WHERE);
  }

  SQLGroupBy *getGroupByNode() {
    return (SQLGroupBy *)findChildNodeByType(SQLNode::GROUPBY);
  }

  SQLHaving *getHavingeNode() {
    return (SQLHaving *)findChildNodeByType(SQLNode::HAVING);
  }
  
  std::string &toString(std::string &buf);

  std::string &toTreeString(std::string &buf);  
};

}  // End parser namespace
}  // End peloton namespace
