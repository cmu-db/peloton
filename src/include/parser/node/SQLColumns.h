/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLCOLUMNS_H_
#define _USQL_SQLCOLUMNS_H_

#include <string>

#include "parser/node/SQLDataSet.h"

namespace uSQL {

class SQLColumns : public SQLDataSet {

public:

  SQLColumns() {
    setType(COLUMNS);
  }

  const char *getTypeName() {
    return "SQLColumns";
  }
  
  std::string &toString(std::string &buf);
};

}

#endif
