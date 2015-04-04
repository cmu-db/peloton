/*-------------------------------------------------------------------------
 *
 * expression_util.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/expression/expression_util.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <string>
#include <vector>

#include "expression/abstract_expression.h"

namespace nstore {
namespace expression {

/** instantiate a typed expression */
AbstractExpression* expressionFactory(json_spirit::Object &obj,
                                      ExpressionType et, ValueType vt, int vs,
                                      AbstractExpression* lc,
                                      AbstractExpression* rc);


// Several helpers used by expressionFactory() and useful to export to testcases.
AbstractExpression * comparisonFactory(ExpressionType et, AbstractExpression*, AbstractExpression*);
AbstractExpression * operatorFactory(ExpressionType et,  AbstractExpression*, AbstractExpression*);
AbstractExpression * constantValueFactory(const Value &val);
AbstractExpression * parameterValueFactory(int idx);
AbstractExpression * tupleValueFactory(int idx);
AbstractExpression *conjunctionFactory(ExpressionType, AbstractExpression*, AbstractExpression*);

// incomparisonFactory() .. would only wrap the ctor and pass the val. vector

std::string getTypeName(ExpressionType type);

/** If the passed vector contains only TupleValueExpression, it
 * returns ColumnIds of them, otherwise NULL.*/
boost::shared_array<int>
convertIfAllTupleValues(const std::vector<AbstractExpression*> &expressions);

/** If the passed vector contains only ParameterValueExpression, it
 * returns ParamIds of them, otherwise NULL.*/
boost::shared_array<int>
convertIfAllParameterValues(const std::vector<AbstractExpression*> &expressions);

} // End expression namespace
} // End nstore namespace

