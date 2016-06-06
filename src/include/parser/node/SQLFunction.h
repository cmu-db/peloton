/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLFUNCTION_H_
#define _USQL_SQLFUNCTION_H_

#include "parser/node/SQLExpression.h"

namespace uSQL {

class SQLFunction : public SQLExpression {

public:

  SQLFunction() {
    setType(FUNCTION);
  }
  
  const char *getTypeName() {
    return "SQLFunction";
  }
  
  std::string &toString(std::string &buf) {
    std::ostringstream oss;
    std::string childNodeStr;
    oss << getValue() << "(" << childNodesToString(childNodeStr, ",") << ")";
    buf = oss.str();
    return buf;
  }
};


}

#endif
