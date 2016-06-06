//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cast_expression.h
//
// Identification: src/include/expression/cast_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>
#include <sstream>

#include "common/logger.h"
#include "common/types.h"

namespace peloton {
namespace expression {

//===--------------------------------------------------------------------===//
// Cast Expression
//===--------------------------------------------------------------------===//

class CastExpression : public AbstractExpression {
 public:
  CastExpression(ValueType type, AbstractExpression *child)
      : AbstractExpression(EXPRESSION_TYPE_CAST, type, child, nullptr) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *econtext) const override {
    PL_ASSERT(GetLeft() != nullptr);
    PL_ASSERT(GetValueType() != VALUE_TYPE_INVALID);
    Value child_value = GetLeft()->Evaluate(tuple1, tuple2, econtext);
    LOG_TRACE("CastExpr: cast %d as %d", child_value.GetValueType(),
              GetValueType());

    switch (GetValueType()) {
      case VALUE_TYPE_VARCHAR:
        return ValueFactory::CastAsString(child_value);
      case VALUE_TYPE_INTEGER:
        return ValueFactory::CastAsInteger(child_value);
      case VALUE_TYPE_DECIMAL:
        return ValueFactory::CastAsDecimal(child_value);
      case VALUE_TYPE_DOUBLE:
        return ValueFactory::CastAsDouble(child_value);
      default:
        LOG_ERROR("Not implemented yet");
        break;
    }

    return child_value;
  }

  /* @setter for the child expr which will be casted into self.type_
   * Sometimes, when this expr is constructed, we cannot.Get the child thus
   * child will set to nullptr. In that case, this method is used to set the
   * child when available.
   **/
  void SetChild(AbstractExpression *child) { m_left = child; }

  /* @setter for the result type
   * Same reason as SetChild()
   **/
  void SetResultType(ValueType type) { m_valueType = type; }

  std::string DebugInfo(const std::string &spacer) const override {
    std::ostringstream buffer;
    buffer << spacer << "Cast as [" << m_type << "]\n";
    if (m_left != nullptr) buffer << m_left->Debug(" " + spacer);
    return (buffer.str());
  }

  AbstractExpression *Copy() const override {
    return new CastExpression(GetValueType(), CopyUtil(m_left));
  }
};

}  // End expression namespace
}  // End peloton namespace
