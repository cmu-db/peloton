//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// subquery_expression.h
//
// Identification: src/backend/expression/subquery_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "backend/expression/abstract_expression.h"

#include <boost/scoped_ptr.hpp>

namespace peloton {
namespace expression {

/**
 * An expression that produces a temp table from a subquery.
 * Note that this expression type's Evaluate method is a little
 * different from the others: Evaluate will return a subquery id,
 * which can then be retrieved from the executor context
 * by invoking its getSubqueryOutputTable method.
 */
class SubqueryExpression : public AbstractExpression {
    public:
    SubqueryExpression(ExpressionType subqueryType,
        int subqueryId,
        const std::vector<int>& paramIdxs,
        const std::vector<int>& otherParamIdxs,
        const std::vector<AbstractExpression*>* tveParams);

    ~SubqueryExpression();

    Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                   executor::ExecutorContext *context) const;

    std::string DebugInfo(const std::string &spacer) const;

  private:
    const int m_subqueryId;

    // The list of parameter indexes that need to be set by this subquery
    // before the expression can be Evaluateuated.
    std::vector<int> m_paramIdxs;

    // The list of non-set parameter indexes that this subquery depends on,
    // also including its child subqueries.
    // This originate at the grandparent levels.
    std::vector<int> m_otherParamIdxs;

    // The list of the corresponding TVE for each parameter index
    boost::scoped_ptr<const std::vector<AbstractExpression*> > m_tveParams;
};

}  // End expression namespace
}  // End peloton namespace

