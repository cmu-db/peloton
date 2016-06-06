#pragma once

#include <string>
#include "parser/node/SQLExpression.h"

namespace peloton {
namespace parser {

class SQLOrder : public SQLExpression {

public:

  static const int UNKOWN;
  static const int ASC;
  static const int DESC;

private:

  int order;
  
public:

  SQLOrder() {
    setType(ORDER);
  }

  const char *getTypeName() {
    return "SQLOrder";
  }
  
  void setOrder(int type);  
  void setOrder(const std::string &order);
  
  int getOrder() {
    return this->order;
  }

  std::string &toString(std::string &buf);
};

}  // End parser namespace
}  // End peloton namespace

