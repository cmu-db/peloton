//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// parameter_value_expression.h
//
// Identification: src/backend/expression/parameter_value_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#ifndef HSTOREPARAMETERVALUEEXPRESSION_H
#define HSTOREPARAMETERVALUEEXPRESSION_H

#include "common/NValue.hpp"

#include "expressions/abstractexpression.h"

#include <vector>
#include <string>
#include <sstream>
#include <cassert>

namespace voltdb {

class ParameterValueExpression : public AbstractExpression {
public:

    // Constructor to initialize the PVE from the static parameter vector
    // from the VoltDBEngine instance. After the construction the PVE points
    // to the NValue from the global vector.
    ParameterValueExpression(int value_idx);

    // Constructor to use for testing purposes
    ParameterValueExpression(int value_idx, voltdb::NValue* paramValue) :
        m_valueIdx(value_idx), m_paramValue(paramValue) {
    }

    voltdb::NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        assert(m_paramValue != NULL);
        return *m_paramValue;
    }

    bool hasParameter() const {
        // this class represents a parameter.
        return true;
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

    voltdb::NValue *m_paramValue;
};

}
#endif
