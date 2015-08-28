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

#include "boost/shared_array.hpp"

namespace peloton {
namespace expression {

class ExpressionUtil {
 public:
  /** instantiate a typed expression */
  static AbstractExpression* expressionFactory(PlannerDomValue obj,
                                               ExpressionType et, ValueType vt, int vs,
                                               AbstractExpression* lc, AbstractExpression* rc,
                                               const std::vector<AbstractExpression*>* arguments);

  static AbstractExpression* comparisonFactory(PlannerDomValue obj,ExpressionType et, AbstractExpression *lc, AbstractExpression *rc);
  static AbstractExpression* conjunctionFactory(ExpressionType et, AbstractExpression *lc, AbstractExpression *rc);

  static void loadIndexedExprsFromJson(std::vector<AbstractExpression*>& indexed_exprs,
                                       const std::string& jsonarraystring);

  static AbstractExpression* loadExpressionFromJson(const std::string& jsonstring);

  /** If the passed vector contains only TupleValueExpression, it
   * returns ColumnIds of them, otherwise NULL.*/
  static boost::shared_array<int>
  convertIfAllTupleValues(const std::vector<AbstractExpression*> &expression);

  /** If the passed vector contains only ParameterValueExpression, it
   * returns ParamIds of them, otherwise NULL.*/
  static boost::shared_array<int>
  convertIfAllParameterValues(const std::vector<AbstractExpression*> &expression);

  /** Returns ColumnIds of TupleValueExpression expression from passed axpression.*/
  static void
  extractTupleValuesColumnIdx(const AbstractExpression* expr, std::vector<int> &columnIds);

  // Implemented in functionexpression.cpp because function expression handling is a system unto itself.
  static AbstractExpression * functionFactory(int functionId, const std::vector<AbstractExpression*>* arguments);

  static AbstractExpression* vectorFactory(ValueType vt, const std::vector<AbstractExpression*>* args);

};

}  // End expression namespace
}  // End peloton namespace
