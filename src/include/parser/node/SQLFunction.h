#pragma once

#include "parser/node/SQLExpression.h"

namespace peloton {
namespace parser {

class SQLFunction : public SQLExpression {

public:

  SQLFunction() {
    setType(FUNCTION);
  }
  
  const char *getTypeName() {
    return "SQLFunction";
  }
  
  std::string &toString(std::string &buf) {
    std::ostringstream oss;
    std::string childNodeStr;
    oss << getValue() << "(" << childNodesToString(childNodeStr, ",") << ")";
    buf = oss.str();
    return buf;
  }
};

}  // End parser namespace
}  // End peloton namespace
