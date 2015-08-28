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

#pragma once

#include "backend/common/value.h"

#include "backend/expression/abstract_expression.h"

#include <vector>
#include <string>
#include <sstream>
#include <cassert>

namespace peloton {
namespace expression {

class ParameterValueExpression : public AbstractExpression {
public:

    // Constructor to initialize the PVE from the static parameter vector
    // from the VoltDBEngine instance. After the construction the PVE points
    // to the Value from the global vector.
    ParameterValueExpression(int value_idx);

    // Constructor to use for testing purposes
    ParameterValueExpression(int value_idx, Value* paramValue) :
        m_valueIdx(value_idx), m_paramValue(paramValue) {
    }

    Value eval(const AbstractTuple *tuple1, const AbstractTuple *tuple2) const {
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

    Value *m_paramValue;
};

}  // End expression namespace
}  // End peloton namespace
