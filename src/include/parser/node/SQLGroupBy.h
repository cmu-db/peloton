/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLGROUPBY_H_
#define _USQL_SQLGROUPBY_H_

#include "parser/node/SQLExpression.h"

namespace uSQL {

class SQLGroupBy : public SQLExpression {

public:

  SQLGroupBy() {
    setType(GROUPBY);
  }

  const char *getTypeName() {
    return "SQLGroupBy";
  }
  
  std::string &toString(std::string &buf) {
    std::ostringstream oss;
    std::string childNodeStr;
    oss << "GROUP BY " << childNodesToString(childNodeStr, ",");
    buf = oss.str();
    return buf;
  }
  
};

}

#endif
