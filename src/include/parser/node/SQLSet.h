/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLSET_H_
#define _USQL_SQLSET_H_

#include "parser/node/SQLExpression.h"

namespace uSQL {

class SQLSet : public SQLExpression {

private:
  
  std::string name;
  
public:

  SQLSet() {
    setType(SET);
  }

  ~SQLSet();
    
  const char *getTypeName() {
    return "SQLSet";
  }
  
  void setName(const std::string &name) {
    this->name = name;
  }
  
  bool hasName() {
    return (0 < this->name.length());
  }
  
  const std::string &getName() {
    return this->name;
  }

  void setValue(SQLExpression *value) {
    clearExpressions();
    addExpression(value);
  }
  
  SQLExpression *getValue() {
    return getExpression(0);
  }
  
  std::string &toString(std::string &buf);
};

}

#endif
