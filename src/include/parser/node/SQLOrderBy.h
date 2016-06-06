#pragma once

#include "parser/SQLNode.h"

namespace peloton {
namespace parser {

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

}  // End parser namespace
}  // End peloton namespace

