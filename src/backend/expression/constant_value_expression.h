//===----------------------------------------------------------------------===//
//
//                         PelotonDB
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

class ConstantValueExpression : public AbstractExpression {
 public:
  ConstantValueExpression(const Value &value)
 : AbstractExpression(EXPRESSION_TYPE_VALUE_CONSTANT) {
    /**
     * A deep copy is desired here because we don't know
     * if the expression will live longer than the passed value
     * or if uninlined value will be freed somewhere else
     */
    this->value = ValueFactory::Clone(value);
  }

  virtual ~ConstantValueExpression() {
    value.Free();
  }

  Value
  Evaluate(__attribute__((unused)) const AbstractTuple *tuple1,
           __attribute__((unused)) const AbstractTuple *tuple2,
           __attribute__((unused)) executor::ExecutorContext *context) const
  {
    LOG_TRACE ("returning constant value as Value:%s type:%d",
               value.Debug().c_str(), (int) this->m_type);
    return this->value;
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "OptimizedConstantValueExpression:" +
        value.Debug() + "\n";
  }

 protected:
  Value value;
};

}  // End expression namespace
}  // End peloton namespace
