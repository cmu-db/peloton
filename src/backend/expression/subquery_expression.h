//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// subquery_expression.h
//
// Identification: src/backend/expression/subquery_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "backend/expression/abstract_expression.h"

namespace peloton {
namespace expression {

/**
 * An expression that produces a temp table from a subquery.
 * Note that this expression type's Evaluate method is a little
 * different from the others: Evaluate will return a subquery id,
 * which can then be retrieved from the executor context
 * by invoking its.GetSubqueryOutputTable method.
 */
class SubqueryExpression : public AbstractExpression {
 public:
  SubqueryExpression(ExpressionType subqueryType, ValueType result_type,
                     int subqueryId, const std::vector<int> &paramIdxs,
                     const std::vector<int> &otherParamIdxs,
                     const std::vector<AbstractExpression *> &tveParams);

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const;

  std::string DebugInfo(const std::string &spacer) const;

  AbstractExpression *Copy() const {
    return new SubqueryExpression(GetExpressionType(), GetValueType(),
                                  m_subqueryId, m_paramIdxs, m_otherParamIdxs,
                                  std::vector<AbstractExpression *>());
  }

 private:
  const int m_subqueryId;

  // The .Ist of parameter indexes that need to be set by this subquery
  // before the expression can be Evaluateuated.
  std::vector<int> m_paramIdxs;

  // The .Ist of non-set parameter indexes that this subquery depends on,
  // also including its child subqueries.
  // T.Is originate at the grandparent levels.
  std::vector<int> m_otherParamIdxs;
};

}  // namespace expression
}  // namespace peloton
