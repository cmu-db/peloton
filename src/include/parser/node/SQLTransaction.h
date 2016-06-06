#pragma once

#include "parser/SQLNode.h"

namespace peloton {
namespace parser {

class SQLTransaction : public SQLNode {

public:

  SQLTransaction() {
    setType(TRANSACTION);
  }

  const char *getTypeName() {
    return "SQLTransaction";
  }
  
};

}  // End parser namespace
}  // End peloton namespace
