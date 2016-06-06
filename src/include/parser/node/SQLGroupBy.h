#pragma once

#include "parser/node/SQLExpression.h"

namespace peloton {
namespace parser {

class SQLGroupBy : public SQLExpression {

public:

  SQLGroupBy() {
    setType(GROUPBY);
  }

  const char *getTypeName() {
    return "SQLGroupBy";
  }
  
  std::string &toString(std::string &buf) {
    std::ostringstream oss;
    std::string childNodeStr;
    oss << "GROUP BY " << childNodesToString(childNodeStr, ",");
    buf = oss.str();
    return buf;
  }
  
};

}  // End parser namespace
}  // End peloton namespace
