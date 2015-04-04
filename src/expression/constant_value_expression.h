
#pragma once

#include "expression/abstract_expression.h"

#include "common/value_vector.h"

#include <string>

namespace nstore {
namespace expression {

class SerializeInput;
class SerializeOutput;

class ConstantValueExpression : public AbstractExpression {
 public:
  ConstantValueExpression(const Value &value)
 : AbstractExpression(EXPRESSION_TYPE_VALUE_CONSTANT) {
    this->value = value;
  }

  virtual ~ConstantValueExpression() {
    // FIXME
    //value.free();
  }

  Value eval(__attribute__((unused)) const storage::Tuple *tuple1, __attribute__((unused)) const storage::Tuple *tuple2) const
  {
    //VOLT_TRACE ("returning constant value as Value:%s type:%d",
    //             value.debug().c_str(), (int) this->m_type);
    return this->value;
  }

  std::string debugInfo(const std::string &spacer) const {
    // FIXME
    return spacer + "OptimizedConstantValueExpression:";
    //value.debug() + "\n";
  }

 protected:
  Value value;
};

} // End expression namespace
} // End nstore namespace

