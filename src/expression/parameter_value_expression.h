
#pragma once

#include "expression/tuple_value_expression.h"
#include "expression/constant_value_expression.h"

#include "common/value_vector.h"

#include <vector>
#include <string>
#include <sstream>

namespace nstore {
namespace expression {

class ParameterValueExpressionMarker {
 public:
  virtual ~ParameterValueExpressionMarker(){}
  virtual int getParameterId() const = 0;
};

class ParameterValueExpression : public AbstractExpression, public ParameterValueExpressionMarker {
 public:

  ParameterValueExpression(int value_idx)
 : AbstractExpression(EXPRESSION_TYPE_VALUE_PARAMETER)
 {
    //VOLT_TRACE("ParameterValueExpression %d", value_idx);
    this->m_valueIdx = value_idx;
 };

  Value eval(__attribute__((unused)) const storage::Tuple *tuple1, __attribute__((unused)) const storage::Tuple *tuple2) const {
    return this->m_paramValue;
  }

  bool hasParameter() const {
    // this class represents a parameter.
    return true;
  }

  void substitute(const ValueArray &params) {
    assert (this->m_valueIdx < params.GetSize());
    m_paramValue = params[this->m_valueIdx];
  }

  std::string debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << spacer << "OptimizedParameter[" << this->m_valueIdx << "]\n";
    return (buffer.str());
  }

  int getParameterId() const {
    return this->m_valueIdx;
  }

 private:
  int m_valueIdx;
  Value m_paramValue;
};

} // End expression namespace
} // End nstore namespace

