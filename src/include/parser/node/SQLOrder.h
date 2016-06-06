/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLORDER_H_
#define _USQL_SQLORDER_H_

#include <string>
#include "parser/node/SQLExpression.h"

namespace uSQL {

class SQLOrder : public SQLExpression {

public:

  static const int UNKOWN;
  static const int ASC;
  static const int DESC;

private:

  int order;
  
public:

  SQLOrder() {
    setType(ORDER);
  }

  const char *getTypeName() {
    return "SQLOrder";
  }
  
  void setOrder(int type);  
  void setOrder(const std::string &order);
  
  int getOrder() {
    return this->order;
  }

  std::string &toString(std::string &buf);
};

}

#endif
