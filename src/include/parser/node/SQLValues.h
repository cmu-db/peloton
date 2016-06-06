#pragma once

#include "parser/node/SQLExpression.h"

namespace peloton {
namespace parser {

class SQLValues : public SQLExpression {

public:

  SQLValues() {
    setType(VALUES);
  }
  
  const char *getTypeName() {
    return "SQLValues";
  }
  
  std::string &toString(std::string &buf);
};

}  // End parser namespace
}  // End peloton namespace
