//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_util.h
//
// Identification: src/expression/expression_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "expression/abstract_expression.h"

namespace peloton {
namespace expression {

class ExpressionUtil {
 public:

  //===--------------------------------------------------------------------===//
  // Factories
  //===--------------------------------------------------------------------===//

  // These helper factories are used by ExpressionFactory()

  static AbstractExpression *OperatorFactory(ExpressionType et, ValueType vt,
                                             AbstractExpression *lc,
                                             AbstractExpression *rc);

  // convert the enumerated value type into a concrete c type for the
  //  operator expression templated ctors
  static AbstractExpression *OperatorFactory(ExpressionType et,
                                             ValueType vt,
                                             AbstractExpression *first,
                                             AbstractExpression *second,
                                             AbstractExpression *third,
                                             AbstractExpression *fourth);

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

  static AbstractExpression *VectorFactory(
      ValueType vt, const std::vector<AbstractExpression *> &args);
  static AbstractExpression *ParameterValueFactory(ValueType type, int idx);

  static AbstractExpression *TupleValueFactory(ValueType type, int tuple_idx,
                                               int value_idx);

  static AbstractExpression *ConstantValueFactory(const Value &newvalue);

  static AbstractExpression *UDFExpressionFactory(
      Oid function_id, Oid collation, Oid return_type,
      std::vector<std::unique_ptr<expression::AbstractExpression>> &args);
};

}  // End expression namespace
}  // End peloton namespace
