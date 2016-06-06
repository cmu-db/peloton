#pragma once

#include "parser/node/SQLDataSet.h"

namespace peloton {
namespace parser {

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

}  // End parser namespace
}  // End peloton namespace


