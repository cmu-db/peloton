
#ifndef HSTOREPARAMETERVALUEEXPRESSION_H
#define HSTOREPARAMETERVALUEEXPRESSION_H

#include "expressions/tuplevalueexpression.h"
#include "expressions/constantvalueexpression.h"

#include "common/valuevector.h"

#include <vector>
#include <string>
#include <sstream>

namespace voltdb {

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
        VOLT_TRACE("ParameterValueExpression %d", value_idx);
        this->m_valueIdx = value_idx;
    };

    voltdb::NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        return this->m_paramValue;
    }

    bool hasParameter() const {
        // this class represents a parameter.
        return true;
    }

    void substitute(const NValueArray &params) {
        assert (this->m_valueIdx < params.size());
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
    voltdb::NValue m_paramValue;
};

}
#endif
