#pragma once

#include "parser/SQLNode.h"

namespace peloton {
namespace parser {

class SQLOffset : public SQLNode {

public:

  SQLOffset() {
    setType(OFFSET);
  }
  
  const char *getTypeName() {
    return "SQLOffset";
  }
  
  std::string &toString(std::string &buf) {
    std::ostringstream oss;
    std::string childNodeStr;
    oss << "OFFSET " << childNodesToString(childNodeStr);
    buf = oss.str();
    return buf;
  }
};

}  // End parser namespace
}  // End peloton namespace
