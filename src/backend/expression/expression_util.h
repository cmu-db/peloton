//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_util.h
//
// Identification: src/backend/expression/expression_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
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
  //===--------------------------------------------------------------------===//
  // Expression Utilities
  //===--------------------------------------------------------------------===//

  // instantiate a typed expression
  static AbstractExpression *ExpressionFactory(
      PlannerDomValue obj, ExpressionType et, ValueType vt, int vs,
      AbstractExpression *lc, AbstractExpression *rc,
      const std::vector<AbstractExpression *> &arguments);

  static AbstractExpression *ExpressionFactory(json_spirit::Object &obj,
                                               ExpressionType et, ValueType vt,
                                               int vs, AbstractExpression *lc,
                                               AbstractExpression *rc);

  //===--------------------------------------------------------------------===//
  // Factories
  //===--------------------------------------------------------------------===//

  // These helper factories are used by ExpressionFactory()

  static AbstractExpression *OperatorFactory(ExpressionType et,
                                             AbstractExpression *lc,
                                             AbstractExpression *rc);

  // convert the enumerated value type into a concrete c type for the
  //  operator expression templated ctors
  static AbstractExpression *OperatorFactory(ExpressionType et,
                                             AbstractExpression *first,
                                             AbstractExpression *second,
                                             AbstractExpression *third,
                                             AbstractExpression *fourth);

  static AbstractExpression *ComparisonFactory(PlannerDomValue obj,
                                               ExpressionType et,
                                               AbstractExpression *lc,
                                               AbstractExpression *rc);

  static AbstractExpression *ComparisonFactory(ExpressionType c,
                                               AbstractExpression *lc,
                                               AbstractExpression *rc);

  static AbstractExpression *ConjunctionFactory(ExpressionType et,
                                                AbstractExpression *lc,
                                                AbstractExpression *rc);

  static AbstractExpression *ConjunctionFactory(
      ExpressionType et, std::list<AbstractExpression *> exprs);

  // If the passed vector contains only TupleValueExpression, it
  // returns ColumnIds of them, other.Ise NULL.
  static boost::shared_array<int> ConvertIfAllTupleValues(
      const std::vector<AbstractExpression *> &expression);

  // If the passed vector contains only ParameterValueExpression, it
  // returns ParamIds of them, other.Ise NULL.
  static boost::shared_array<int> ConvertIfAllParameterValues(
      const std::vector<AbstractExpression *> &expression);

  // Returns ColumnIds of TupleValueExpression expression from passed
  // axpression.
  static void ExtractTupleValuesColumnIdx(const AbstractExpression *expr,
                                          std::vector<int> &columnIds);

  // Implemented in functionexpression.cpp because function expression
  // handling.Is a system unto itself.
  static AbstractExpression *FunctionFactory(
      int functionId, const std::vector<AbstractExpression *> &arguments);

  static AbstractExpression *CastFactory(ValueType vt, AbstractExpression *lc);

  static AbstractExpression *CastFactory(
      PostgresValueType type = POSTGRES_VALUE_TYPE_INVALID,
      AbstractExpression *child = nullptr);

  static AbstractExpression *VectorFactory(
      ValueType vt, const std::vector<AbstractExpression *> &args);
  static AbstractExpression *ParameterValueFactory(int idx);
  static AbstractExpression *ParameterValueFactory(PlannerDomValue obj,
                                                   ExpressionType et,
                                                   AbstractExpression *lc,
                                                   AbstractExpression *rc);
  static AbstractExpression *ParameterValueFactory(json_spirit::Object &obj,
                                                   ExpressionType et,
                                                   AbstractExpression *lc,
                                                   AbstractExpression *rc);
  static AbstractExpression *TupleValueFactory(int tuple_idx, int value_idx);
  static AbstractExpression *TupleValueFactory(PlannerDomValue obj,
                                               ExpressionType et,
                                               AbstractExpression *lc,
                                               AbstractExpression *rc);

  // convert the enumerated value type into a concrete c type for
  // tuple value expression templated ctors
  static AbstractExpression *TupleValueFactory(json_spirit::Object &obj,
                                               ExpressionType et,
                                               AbstractExpression *lc,
                                               AbstractExpression *rc);

  static AbstractExpression *HashRangeFactory(PlannerDomValue obj);

  static AbstractExpression *SubqueryFactory(
      ExpressionType subqueryType, PlannerDomValue obj,
      const std::vector<AbstractExpression *> &rgs);

  static AbstractExpression *ConstantValueFactory(PlannerDomValue obj,
                                                  ValueType vt,
                                                  ExpressionType et,
                                                  AbstractExpression *lc,
                                                  AbstractExpression *rc);

  static AbstractExpression *ConstantValueFactory(const Value &newvalue);

  // convert the enumerated value type into a concrete c type for
  // constant value expressions templated ctors
  static AbstractExpression *ConstantValueFactory(json_spirit::Object &obj,
                                                  ValueType vt,
                                                  ExpressionType et,
                                                  AbstractExpression *lc,
                                                  AbstractExpression *rc);
};

}  // End expression namespace
}  // End peloton namespace
