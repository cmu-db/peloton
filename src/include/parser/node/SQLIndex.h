/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLINDEX_H_
#define _USQL_SQLINDEX_H_

#include "parser/SQLNode.h"

namespace uSQL {

class SQLIndex : public SQLNode {

public:

  SQLIndex() {
    setType(INDEX);
  }

  const char *getTypeName() {
    return "SQLIndex";
  }
};

}
#endif
