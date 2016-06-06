/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLCOLLECTION_H_
#define _USQL_SQLCOLLECTION_H_

#include "parser/SQLNode.h"

namespace uSQL {

class SQLCollection : public SQLNode {

public:

  SQLCollection() {
    setType(COLLECTION);
  }

  const char *getTypeName() {
    return "SQLCollection";
  }
};

}

#endif
