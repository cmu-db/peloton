/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLORDERBY_H_
#define _USQL_SQLORDERBY_H_

#include "parser/SQLNode.h"

namespace uSQL {

class SQLOrderBy : public SQLNode {

public:

  SQLOrderBy() {
    setType(ORDERBY);
  }

  const char *getTypeName() {
    return "SQLOrderBy";
  }
  
  
  std::string &toString(std::string &buf) {
    std::ostringstream oss;
    std::string childNodeStr;
    oss << "ORDER BY " << childNodesToString(childNodeStr, ",");
    buf = oss.str();
    return buf;
  }
  
};

}

#endif
