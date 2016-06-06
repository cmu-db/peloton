#pragma once

#include "parser/SQLNode.h"

namespace peloton {
namespace parser {

class SQLLimit : public SQLNode {

public:

  SQLLimit() {
    setType(LIMIT);
  }
  
  const char *getTypeName() {
    return "SQLLimit";
  }
  
  std::string &toString(std::string &buf) {
    std::ostringstream oss;
    std::string childNodeStr;
    oss << "LIMIT " << childNodesToString(childNodeStr, ",");
    buf = oss.str();
    return buf;
  }
};

}  // End parser namespace
}  // End peloton namespace
