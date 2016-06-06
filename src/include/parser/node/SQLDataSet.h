#pragma once

#include "parser/node/SQLExpression.h"

namespace peloton {
namespace parser {

class SQLDataSet : public SQLExpression {

public:

  SQLDataSet() {
  }

  virtual ~SQLDataSet() {
  }

};

}  // End parser namespace
}  // End peloton namespace
