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

    voltdb::Value eval(const TableTuple *tuple1, const TableTuple *tuple2) const;

    std::string debugInfo(const std::string &spacer) const;

};

}  // End expression namespace
}  // End peloton namespace
