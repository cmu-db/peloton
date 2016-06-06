/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLEXPRESSION_H_
#define _USQL_SQLEXPRESSION_H_

#include "parser/SQLNode.h"

namespace uSQL {

class SQLExpression : public SQLNode {

  //std::string name;  
  int literalType;
  
public:

  static const int UNKOWN;
  static const int STRING;
  static const int PROPERTY;
  static const int INTEGER;
  static const int REAL;
  static const int BOOLEAN;
  static const int OPERATOR;
  static const int FUNCTION;
  static const int SELECT;
  static const int BLOB;
  static const int NIL;
  static const int CURRENT_TIME;
  static const int CURRENT_DATE;
  static const int CURRENT_TIMESTAMP;
  static const int ASTERISK;
  
protected:

  std::string &toExpressionString(std::string &buf);

public:

  SQLExpression() {
    setType(EXPRESSION);
    setLiteralType(UNKOWN);
  }
  
  ~SQLExpression() {
  }
    
  const char *getTypeName() {
    return "SQLExpression";
  }
  
  void setLiteralType(int type);
  
  int getLiteralType() {
    return this->literalType;
  }
  
  bool isLiteralType(int type) {
    return (this->literalType == type) ? true : false;
  }
  
  /*
  void setName(const std::string &value) {
    this->name = value;
  }
  
  const std::string &getName() {
    return this->name;
  }
  
  bool isDictionary()
  {
    return (this->name.length()) ? true : false;
  }
  */
   
  void set(SQLExpression *exprNode);
  
  bool isFunction()
  {
    return isLiteralType(FUNCTION);
  }

  bool isOperator()
  {
    return isLiteralType(OPERATOR);
  }
  
  bool isAsterisk()
  {
    return isLiteralType(ASTERISK);
  }
  
  void addExpression(SQLExpression *expr) {
    addChildNode(expr);
  }
  
  int getExpressionCount() {
    return getChildCount();
  }
  
  SQLNodeList *getExpressions() {
    return getChildNodes();
  }
  
  SQLExpression *getExpression(size_t index) {
    return (SQLExpression *)getChildNode(index);
  }
  
  bool hasExpressions() {
    SQLNodeList *expressions = getExpressions();
    return (0 < expressions->size()) ? true : false;
  }
  
  void clearExpressions() {
    clearChildNodes();
  }
  
};

class SQLAsterisk : public SQLExpression {

public:

  SQLAsterisk() {
    setLiteralType(ASTERISK);
    setValue("*");
  }

};

}
#endif
