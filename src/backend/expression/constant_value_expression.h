//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// constant_value_expression.h
//
// Identification: src/backend/expression/constant_value_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/expression/abstract_expression.h"
#include "backend/common/value_vector.h"

#include <string>

namespace peloton {
namespace expression {

class ConstantValueExpression : public AbstractExpression {
    public:
    ConstantValueExpression(const Value &value)
        : AbstractExpression(EXPRESSION_TYPE_VALUE_CONSTANT) {
        this->value = value;
    }

    virtual ~ConstantValueExpression() {
        value.free();
    }

    Value
    eval(const AbstractTuple *tuple1, const TableTuple *tuple2) const
    {
        VOLT_TRACE ("returning constant value as Value:%s type:%d",
                     value.debug().c_str(), (int) this->m_type);
        return this->value;
    }

    std::string debugInfo(const std::string &spacer) const {
        return spacer + "OptimizedConstantValueExpression:" +
          value.debug() + "\n";
    }

  protected:
    Value value;
};

}  // End expression namespace
}  // End peloton namespace
