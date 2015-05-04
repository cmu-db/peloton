
#pragma once

#include "expression/abstract_expression.h"

#include "common/value_vector.h"

#include <string>

namespace nstore {
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

  Value Evaluate(__attribute__((unused)) const Tuple *tuple1, __attribute__((unused)) const Tuple *tuple2) const {
    return this->value;
  }

  std::string DebugInfo(const std::string &spacer) const {
    std::stringstream os;
    os << spacer << "OptimizedConstantValueExpression:" << value;
    return os.str();
  }

  Value GetValue(){
    return value;
  }

 protected:
  Value value;
};

} // End expression namespace
} // End nstore namespace

