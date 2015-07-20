
#pragma once

#include <vector>
#include <string>
#include <sstream>

#include "backend/common/value_vector.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/constant_value_expression.h"

namespace peloton {
namespace expression {

//===--------------------------------------------------------------------===//
// Parameter Expression
//===--------------------------------------------------------------------===//

class ParameterValueExpressionMarker {
 public:
  virtual ~ParameterValueExpressionMarker(){}
  virtual int GetParameterId() const = 0;
};

class ParameterValueExpression : public AbstractExpression, public ParameterValueExpressionMarker {
 public:

  ParameterValueExpression(int value_idx)
 : AbstractExpression(EXPRESSION_TYPE_VALUE_PARAMETER) {
    this->m_valueIdx = value_idx;
 };

  Value Evaluate(__attribute__((unused)) const AbstractTuple *tuple1,
                 __attribute__((unused)) const AbstractTuple *tuple2,
                 executor::ExecutorContext* econtext) const {

    assert(econtext);

    auto& params = econtext->GetParams();
    return params[this->m_valueIdx];
  }

  bool HasParameter() const {
    // this class represents a parameter.
    return true;
  }

  void Substitute(const ValueArray &params) {
    assert (this->m_valueIdx < params.GetSize());
    m_paramValue = params[this->m_valueIdx];
  }

  std::string DebugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << spacer << "OptimizedParameter[" << this->m_valueIdx << "]\n";
    return (buffer.str());
  }

  int GetParameterId() const {
    return this->m_valueIdx;
  }

 private:
  int m_valueIdx;
  Value m_paramValue;
};

} // End expression namespace
} // End peloton namespace

