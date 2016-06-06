#pragma once

#include "parser/SQLNode.h"

namespace peloton {
namespace parser {

class SQLCollection : public SQLNode {

public:

  SQLCollection() {
    setType(COLLECTION);
  }

  const char *getTypeName() {
    return "SQLCollection";
  }
};

}  // End parser namespace
}  // End peloton namespace
