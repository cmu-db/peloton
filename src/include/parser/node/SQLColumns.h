#pragma once

#include <string>

#include "parser/node/SQLDataSet.h"

namespace peloton {
namespace parser {

class SQLColumns : public SQLDataSet {

public:

  SQLColumns() {
    setType(COLUMNS);
  }

  const char *getTypeName() {
    return "SQLColumns";
  }
  
  std::string &toString(std::string &buf);
};

}  // End parser namespace
}  // End peloton namespace
