//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// scalar_value_expression.h
//
// Identification: src/backend/expression/scalar_value_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/expression/abstract_expression.h"

#include <string>

namespace peloton {
namespace expression {

class ScalarValueExpression : public AbstractExpression {
  public:
    ScalarValueExpression(AbstractExpression *lc)
        : AbstractExpression(EXPRESSION_TYPE_VALUE_SCALAR, lc, NULL)
    {};

    Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                   executor::ExecutorContext *context) const;

    std::string DebugInfo(const std::string &spacer) const;

};

}  // End expression namespace
}  // End peloton namespace
