//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// expression_util.h
//
// Identification: src/backend/expression/expression_util.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "backend/expression/abstract_expression.h"

namespace peloton {
namespace expression {

//===--------------------------------------------------------------------===//
// Expression Utilities
//===--------------------------------------------------------------------===//

// instantiate a typed expression
AbstractExpression *ExpressionFactory(json_spirit::Object &obj,
                                      ExpressionType et, ValueType vt, int vs,
                                      AbstractExpression *lc,
                                      AbstractExpression *rc);

//===--------------------------------------------------------------------===//
// Factories
//===--------------------------------------------------------------------===//

// Several helpers used by expressionFactory() and useful to export to
// testcases.

AbstractExpression *ComparisonFactory(ExpressionType et, AbstractExpression *,
                                      AbstractExpression *);

AbstractExpression *OperatorFactory(ExpressionType et, AbstractExpression *,
                                    AbstractExpression *);

AbstractExpression *ConstantValueFactory(const peloton::Value &val);

AbstractExpression *ParameterValueFactory(int idx);

AbstractExpression *TupleValueFactory(int tuple_idx, int value_idx);

AbstractExpression *ConjunctionFactory(ExpressionType, AbstractExpression *,
                                       AbstractExpression *);
AbstractExpression *ConjunctionFactory(ExpressionType,
                                       std::list<AbstractExpression *>);

AbstractExpression *CastFactory(PostgresValueType type=POSTGRES_VALUE_TYPE_INVALID, AbstractExpression *child=nullptr);

// If the passed vector contains only TupleValueExpression, it
// returns ColumnIds of them, otherwise NULL.
boost::shared_array<int> ConvertIfAllTupleValues(
    const std::vector<AbstractExpression *> &expressions);

// If the passed vector contains only ParameterValueExpression, it
// returns ParamIds of them, otherwise NULL.
boost::shared_array<int> ConvertIfAllParameterValues(
    const std::vector<AbstractExpression *> &expressions);

}  // End expression namespace
}  // End peloton namespace
