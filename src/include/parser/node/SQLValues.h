/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLVALUES_H_
#define _USQL_SQLVALUES_H_

#include "parser/node/SQLExpression.h"

namespace uSQL {

class SQLValues : public SQLExpression {

public:

  SQLValues() {
    setType(VALUES);
  }
  
  const char *getTypeName() {
    return "SQLValues";
  }
  
  std::string &toString(std::string &buf);
};

}

#endif
