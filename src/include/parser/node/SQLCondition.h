#pragma once

#include "parser/node/SQLExpression.h"

namespace peloton {
namespace parser {

class SQLCondition : public SQLExpression {

public:

  SQLCondition() {
    setType(CONDITION);
  }
  
  const char *getTypeName() {
    return "SQLCondition";
  }
  
  virtual std::string &toString(std::string &buf) = 0;
 };

class SQLWhere : public SQLCondition {

public:

  SQLWhere() {
    setType(WHERE);
  }
  
  const char *getTypeName() {
    return "SQLWhere";
  }
  
  std::string &toString(std::string &buf) {
    std::string exprString;
    std::ostringstream oss;
    oss << "WHERE " << SQLExpression::toString(exprString) ;
    buf = oss.str();
    return buf;
  }
};

class SQLHaving : public SQLExpression {

public:

  SQLHaving() {
    setType(HAVING);
  }

  const char *getTypeName() {
    return "SQLHaving";
  }
  
  std::string &toString(std::string &buf) {
    std::ostringstream oss;
    std::string childNodeStr;
    oss << "HAVING " << childNodesToString(childNodeStr);
    buf = oss.str();
    return buf;
  }
  
};

}  // End parser namespace
}  // End peloton namespace
