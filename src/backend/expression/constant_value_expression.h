//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constant_value_expression.h
//
// Identification: src/backend/expression/constant_value_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/expression/abstract_expression.h"
#include "backend/common/value_factory.h"

#include <string>
#include <sstream>

namespace peloton {
namespace expression {

class ConstantValueExpression : public AbstractExpression {
 public:
  ConstantValueExpression(const Value &value)
      : AbstractExpression(EXPRESSION_TYPE_VALUE_CONSTANT,
                           value.GetValueType()) {
    /**
     * A deep copy is desired here because we don't know
     * if the expression will live longer than the passed value
     * or if uninlined value will be freed somewhere else
     */
    this->value = ValueFactory::Clone(value, nullptr);
  }

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
                 UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
                 UNUSED_ATTRIBUTE
                 executor::ExecutorContext *context) const override {
    LOG_TRACE("returning constant value as Value:%s type:%d",
              value.GetInfo().c_str(), (int)this->m_type);
    return this->value;
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return spacer + "OptimizedConstantValueExpression:" + value.GetInfo() +
           "\n";
  }

  AbstractExpression *Copy() const override {
    return new ConstantValueExpression(value);
  }

 protected:
  Value value;
};

}  // End expression namespace
}  // End peloton namespace
