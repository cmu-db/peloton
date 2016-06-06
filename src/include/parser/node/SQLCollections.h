/******************************************************************
*
* uSQL for C++
*
* Copyright (C) Satoshi Konno 2012
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

#ifndef _USQL_SQLCOLLECTIONS_H_
#define _USQL_SQLCOLLECTIONS_H_

#include "parser/node/SQLDataSet.h"

namespace uSQL {

class SQLCollection;

class SQLCollections : public SQLDataSet {

public:

  SQLCollections() {
    setType(COLLECTIONS);
  }

  const char *getTypeName() {
    return "SQLCollections";
  }
  
  SQLCollection *getCollectionNode(size_t index) {
    return (SQLCollection *)getChildNode(index);
  }
  
  std::string &toString(std::string &buf) {
    std::ostringstream oss;
    std::string childNodeStr;
    oss << childNodesToString(childNodeStr, ",");
    buf = oss.str();
    return buf;
  }
  
};

class SQLFrom : public SQLCollections {

public:

  SQLFrom() {
  }

  std::string &toString(std::string &buf) {
    std::ostringstream oss;
    std::string childNodeStr;
    oss << "FROM " << SQLCollections::toString(childNodeStr);
    buf = oss.str();
    return buf;
  }

};

}

#endif
