/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLOPTION_H_
#define _USQL_SQLOPTION_H_

#include "parser/node/SQLExpression.h"

namespace uSQL {

class SQLOption : public SQLExpression {

public:

  SQLOption() {
    setType(OPTION);
  }
  
  const char *getTypeName() {
    return "SQLOption";
  }
  
  std::string &toString(std::string &buf) {
    std::string exprString;
    std::ostringstream oss;
    oss << "OPTION " << SQLExpression::toString(exprString) ;
    buf = oss.str();
    return buf;
  }
};

}

#endif
