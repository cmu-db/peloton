#pragma once

#include "parser/SQLNode.h"

namespace peloton {
namespace parser {

class SQLIndex : public SQLNode {

public:

  SQLIndex() {
    setType(INDEX);
  }

  const char *getTypeName() {
    return "SQLIndex";
  }
};

}  // End parser namespace
}  // End peloton namespace
