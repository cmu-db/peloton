
#ifndef HSTORECONSTANTVALUEEXPRESSION_H
#define HSTORECONSTANTVALUEEXPRESSION_H

#include "expressions/abstractexpression.h"

#include "common/valuevector.h"

#include <string>

namespace voltdb {

class SerializeInput;
class SerializeOutput;

class ConstantValueExpression : public AbstractExpression {
    public:
    ConstantValueExpression(const NValue &value)
        : AbstractExpression(EXPRESSION_TYPE_VALUE_CONSTANT) {
        this->value = value;
    }

    virtual ~ConstantValueExpression() {
        value.free();
    }

    voltdb::NValue
    eval(const TableTuple *tuple1, const TableTuple *tuple2) const
    {
        VOLT_TRACE ("returning constant value as NValue:%s type:%d",
                     value.debug().c_str(), (int) this->m_type);
        return this->value;
    }

    std::string debugInfo(const std::string &spacer) const {
        return spacer + "OptimizedConstantValueExpression:" +
          value.debug() + "\n";
    }

  protected:
    voltdb::NValue value;
};
}
#endif
