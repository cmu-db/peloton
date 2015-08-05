//===----------------------------------------------------------------------===//
//
//							PelotonDB
//
// constant_value_expression.h
//
// Identification: src/backend/expression/constant_value_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


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
  ConstantValueExpression(const Value &cvalue)
 : AbstractExpression(EXPRESSION_TYPE_VALUE_CONSTANT) {
    /**
     * A deep copy is desired here because we don't know
     * if the expression will live longer than the passed value
     * or if uninlined value will be freed somewhere else
     * (and probably yes in production).
     */
    this->value = ValueFactory::Clone(cvalue);
  }

  virtual ~ConstantValueExpression() {
    // clean up
    value.FreeUninlinedData();
  }

  Value Evaluate(__attribute__((unused)) const AbstractTuple *tuple1,
                 __attribute__((unused)) const AbstractTuple *tuple2,
                 __attribute__((unused)) executor::ExecutorContext *) const {
    return this->value;
  }

  std::string DebugInfo(const std::string &spacer) const {
    std::stringstream os;
    os << spacer << "ConstantValueExpression: " << value << "\n";
    return os.str();
  }

  Value GetValue() { return value; }

 protected:
  Value value;
};

}  // End expression namespace
}  // End peloton namespace
