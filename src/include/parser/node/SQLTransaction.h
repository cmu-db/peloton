/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLTRANSACTION_H_
#define _USQL_SQLTRANSACTION_H_

#include "parser/SQLNode.h"

namespace uSQL {

class SQLTransaction : public SQLNode {

public:

  SQLTransaction() {
    setType(TRANSACTION);
  }

  const char *getTypeName() {
    return "SQLTransaction";
  }
  
};

}
#endif
