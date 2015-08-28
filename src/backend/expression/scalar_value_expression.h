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

#ifndef HSTORESCALARVALUEEXPRESSION_H
#define HSTORESCALARVALUEEXPRESSION_H

#include "expressions/abstractexpression.h"

#include <string>

namespace voltdb {

class ScalarValueExpression : public AbstractExpression {
  public:
    ScalarValueExpression(AbstractExpression *lc)
        : AbstractExpression(EXPRESSION_TYPE_VALUE_SCALAR, lc, NULL)
    {};

    voltdb::NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const;

    std::string debugInfo(const std::string &spacer) const;

};

}
#endif
