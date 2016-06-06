#pragma once

#include "parser/node/SQLExpression.h"

namespace peloton {
namespace parser {

class SQLSet : public SQLExpression {

private:
  
  std::string name;
  
public:

  SQLSet() {
    setType(SET);
  }

  ~SQLSet();
    
  const char *getTypeName() {
    return "SQLSet";
  }
  
  void setName(const std::string &name) {
    this->name = name;
  }
  
  bool hasName() {
    return (0 < this->name.length());
  }
  
  const std::string &getName() {
    return this->name;
  }

  void setValue(SQLExpression *value) {
    clearExpressions();
    addExpression(value);
  }
  
  SQLExpression *getValue() {
    return getExpression(0);
  }
  
  std::string &toString(std::string &buf);
};

}  // End parser namespace
}  // End peloton namespace
