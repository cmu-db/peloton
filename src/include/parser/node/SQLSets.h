#pragma once

#include "parser/node/SQLDataSet.h"
#include "parser/node/SQLSet.h"

namespace peloton {
namespace parser {

class SQLSets : public SQLDataSet {

public:

  SQLSets() {
    setType(SETS);
  }
  
  const char *getTypeName() {
    return "SQLSets";
  }
  
  
  int getSetCount() {
    return getChildCount();
  }
  
  SQLNodeList *getSets() {
    return getChildNodes();
  }
  
  SQLSet *getSet(size_t index) {
    return (SQLSet *)getChildNode(index);
  }
  
  std::string &toString(std::string &buf);
};

}  // End parser namespace
}  // End peloton namespace
