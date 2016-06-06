/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLSETS_H_
#define _USQL_SQLSETS_H_

#include "parser/node/SQLDataSet.h"
#include "parser/node/SQLSet.h"

namespace uSQL {

class SQLSets : public SQLDataSet {

public:

  SQLSets() {
    setType(SETS);
  }
  
  const char *getTypeName() {
    return "SQLSets";
  }
  
  
  int getSetCount() {
    return getChildCount();
  }
  
  SQLNodeList *getSets() {
    return getChildNodes();
  }
  
  SQLSet *getSet(size_t index) {
    return (SQLSet *)getChildNode(index);
  }
  
  std::string &toString(std::string &buf);
};

}

#endif
