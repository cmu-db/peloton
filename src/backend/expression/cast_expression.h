//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// cast_expression.h
//
// Identification: src/backend/expression/cast_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>
#include <sstream>

#include "backend/common/value_vector.h"
#include "backend/common/logger.h"

namespace peloton {
namespace expression {

//===--------------------------------------------------------------------===//
// Cast Expression
//===--------------------------------------------------------------------===//

class CastExpression : public AbstractExpression {
 public:
  CastExpression(PostgresValueType type, AbstractExpression *child)
      : AbstractExpression(EXPRESSION_TYPE_CAST), type_(type), child_(child){};

  Value Evaluate(const AbstractTuple *tuple1,
                 const AbstractTuple *tuple2,
                 executor::ExecutorContext *econtext) const {
    assert(this->child_);
    Value child_value = this->child_->Evaluate(tuple1, tuple2, econtext);
    Value casted_value = child_value;
    switch (this->type_) {
      case POSTGRES_VALUE_TYPE_BPCHAR:
      case POSTGRES_VALUE_TYPE_VARCHAR2:
      case POSTGRES_VALUE_TYPE_TEXT:
        casted_value = ValueFactory::CastAsString(child_value);
       break;
      case POSTGRES_VALUE_TYPE_INTEGER:
        casted_value = ValueFactory::CastAsInteger(child_value);
        break;
      case POSTGRES_VALUE_TYPE_DECIMAL:
        casted_value = ValueFactory::CastAsDecimal(child_value);
        break;
      case POSTGRES_VALUE_TYPE_DOUBLE:
        casted_value = ValueFactory::CastAsDouble(child_value);
        break;
      default:
        LOG_ERROR("Not implemented yet, cast as %d", this->type_);
        break;
    }
    LOG_TRACE("cast from %d to %d", child_value.GetValueType(),
    casted_value.GetValueType());

    return casted_value;
  }

  /* @setter for the child expr which will be casted into self.type_
   * Sometimes, when this expr is constructed, we cannot get the child thus child will
   * set to nullptr. In that case, this method is used to set the child when available.
   **/
  void SetChild(AbstractExpression *child) {
    child_ = child;
  }

   /* @setter for the result type
    * Same reason as SetChild()
    **/
  void SetResultType(PostgresValueType type) {
    type_ = type;
  }

  std::string DebugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << spacer << "Cast as [" << this->type_ << "]\n";
    return (buffer.str());
  }

 private:
  PostgresValueType type_;
  AbstractExpression *child_;
};

}  // End expression namespace
}  // End peloton namespace
