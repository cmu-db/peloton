

#ifndef HSTORECONJUNCTIONEXPRESSION_H
#define HSTORECONJUNCTIONEXPRESSION_H

#include "common/common.h"
#include "common/serializeio.h"
#include "common/valuevector.h"

#include "expressions/abstractexpression.h"

#include <string>

namespace voltdb {

class ConjunctionAnd;
class ConjunctionOr;

template <typename C>
class ConjunctionExpression : public AbstractExpression
{
  public:
    ConjunctionExpression(ExpressionType type,
                                   AbstractExpression *left,
                                   AbstractExpression *right)
        : AbstractExpression(type, left, right)
    {
        this->m_left = left;
        this->m_right = right;
    }

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const;

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "ConjunctionExpression\n");
    }

    AbstractExpression *m_left;
    AbstractExpression *m_right;
};

template<> inline NValue
ConjunctionExpression<ConjunctionAnd>::eval(const TableTuple *tuple1,
                                            const TableTuple *tuple2) const
{
    return m_left->eval(tuple1, tuple2).op_and(m_right->eval(tuple1, tuple2));
}

template<> inline NValue
ConjunctionExpression<ConjunctionOr>::eval(const TableTuple *tuple1,
                                           const TableTuple *tuple2) const
{
    return m_left->eval(tuple1, tuple2).op_or(m_right->eval(tuple1, tuple2));
}

}
#endif
