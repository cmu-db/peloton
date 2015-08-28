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

#ifndef HSTORESUBQUERYEXPRESSION_H
#define HSTORESUBQUERYEXPRESSION_H

#include "expressions/abstractexpression.h"

#include <boost/scoped_ptr.hpp>

#include <vector>

namespace voltdb {

/**
 * An expression that produces a temp table from a subquery.
 * Note that this expression type's eval method is a little
 * different from the others: eval will return a subquery id,
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

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const;

    std::string debugInfo(const std::string &spacer) const;

  private:
    const int m_subqueryId;

    // The list of parameter indexes that need to be set by this subquery
    // before the expression can be evaluated.
    std::vector<int> m_paramIdxs;

    // The list of non-set parameter indexes that this subquery depends on,
    // also including its child subqueries.
    // This originate at the grandparent levels.
    std::vector<int> m_otherParamIdxs;

    // The list of the corresponding TVE for each parameter index
    boost::scoped_ptr<const std::vector<AbstractExpression*> > m_tveParams;
};

}
#endif
