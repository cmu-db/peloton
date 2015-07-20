
#pragma once

#include "backend/expression/abstract_expression.h"

#include "backend/common/value_vector.h"

#include <string>

namespace peloton {
namespace expression {

class SerializeInput;
class SerializeOutput;

//===--------------------------------------------------------------------===//
// Constant Value Expression
//===--------------------------------------------------------------------===//

class ConstantValueExpression : public AbstractExpression {
 public:
  ConstantValueExpression(const Value &value)
 : AbstractExpression(EXPRESSION_TYPE_VALUE_CONSTANT) {
    this->value = value;

  }

  virtual ~ConstantValueExpression() {
    // clean up
    value.FreeUninlinedData();
  }

  Value Evaluate(__attribute__((unused)) const AbstractTuple *tuple1,
                 __attribute__((unused)) const AbstractTuple *tuple2,
                 __attribute__((unused)) executor::ExecutorContext*) const {
    return this->value;
  }

  std::string DebugInfo(const std::string &spacer) const {
    std::stringstream os;
    os << spacer << "ConstantValueExpression: " << value;
    return os.str();
  }

  Value GetValue(){
    return value;
  }

 protected:
  Value value;
};

} // End expression namespace
} // End peloton namespace

