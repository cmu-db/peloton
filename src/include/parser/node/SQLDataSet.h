/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLDATASET_H_
#define _USQL_SQLDATASET_H_

#include "parser/node/SQLExpression.h"

namespace uSQL {

class SQLDataSet : public SQLExpression {

public:

  SQLDataSet() {
  }

  virtual ~SQLDataSet() {
  }

};

}

#endif
