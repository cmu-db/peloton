#pragma once

#include "common/serializer.h"
#include "common/value_vector.h"

#include "expression/abstract_expression.h"

#include <string>

namespace nstore {
namespace expression {

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

    Value eval(const storage::Tuple *tuple1, const storage::Tuple *tuple2) const;

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "ConjunctionExpression\n");
    }

    AbstractExpression *m_left;
    AbstractExpression *m_right;
};

template<> inline Value
ConjunctionExpression<ConjunctionAnd>::eval(const storage::Tuple *tuple1,
                                            const storage::Tuple *tuple2) const
{
    return m_left->eval(tuple1, tuple2).OpAnd(m_right->eval(tuple1, tuple2));
}

template<> inline Value
ConjunctionExpression<ConjunctionOr>::eval(const storage::Tuple *tuple1,
                                           const storage::Tuple *tuple2) const
{
    return m_left->eval(tuple1, tuple2).OpOr(m_right->eval(tuple1, tuple2));
}

} // End expression namespace
} // End nstore namespace
