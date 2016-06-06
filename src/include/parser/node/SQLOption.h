#pragma once

#include "parser/node/SQLExpression.h"

namespace peloton {
namespace parser {

class SQLOption : public SQLExpression {

public:

  SQLOption() {
    setType(OPTION);
  }
  
  const char *getTypeName() {
    return "SQLOption";
  }
  
  std::string &toString(std::string &buf) {
    std::string exprString;
    std::ostringstream oss;
    oss << "OPTION " << SQLExpression::toString(exprString) ;
    buf = oss.str();
    return buf;
  }
};

}  // End parser namespace
}  // End peloton namespace
