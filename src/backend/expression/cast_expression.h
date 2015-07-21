#pragma once

#include <vector>
#include <string>
#include <sstream>

#include "backend/common/value_vector.h"
#include "backend/common/logger.h"

namespace peloton {
namespace expression {

//===--------------------------------------------------------------------===//
// Parameter Expression
//===--------------------------------------------------------------------===//

class CastExpression : public AbstractExpression {
 public:

  CastExpression(PostgresValueType type, AbstractExpression *child)
 : AbstractExpression(EXPRESSION_TYPE_CAST),
   type_(type),
   child_(child) {};

  Value Evaluate(__attribute__((unused)) const AbstractTuple *tuple1,
                 __attribute__((unused)) const AbstractTuple *tuple2,
                 executor::ExecutorContext* econtext) const {

    assert(this->child_);
    Value child_value = this->child_->Evaluate(nullptr, nullptr, econtext);
    Value casted_value = child_value;
    switch (this->type_) {
      case POSTGRES_VALUE_TYPE_VARCHAR2:
      case POSTGRES_VALUE_TYPE_TEXT:
        casted_value = ValueFactory::CastAsString(child_value);
        LOG_TRACE("cast from %d to %d", child_value.GetValueType(), casted_value.GetValueType());
        break;
      default:
        LOG_ERROR("Not implemented yet, cast as %d", this->type_);
        break;
    }
    return casted_value;
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

} // End expression namespace
} // End peloton namespace

