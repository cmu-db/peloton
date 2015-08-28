//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// conjunction_expression.h
//
// Identification: src/backend/expression/conjunction_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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
    NValue leftBool = m_left->eval(tuple1, tuple2);
    // False False -> False
    // False True  -> False
    // False NULL  -> False
    if (leftBool.isFalse()) {
        return leftBool;
    }
    NValue rightBool = m_right->eval(tuple1, tuple2);
    // True  False -> False
    // True  True  -> True
    // True  NULL  -> NULL
    // NULL  False -> False
    if (leftBool.isTrue() || rightBool.isFalse()) {
        return rightBool;
    }
    // NULL  True  -> NULL
    // NULL  NULL  -> NULL
    return NValue::getNullValue(VALUE_TYPE_BOOLEAN);
}

template<> inline NValue
ConjunctionExpression<ConjunctionOr>::eval(const TableTuple *tuple1,
                                           const TableTuple *tuple2) const
{
    NValue leftBool = m_left->eval(tuple1, tuple2);
    // True True  -> True
    // True False -> True
    // True NULL  -> True
    if (leftBool.isTrue()) {
        return leftBool;
    }
    NValue rightBool = m_right->eval(tuple1, tuple2);
    // False True  -> True
    // False False -> False
    // False NULL  -> NULL
    // NULL  True  -> True
    if (leftBool.isFalse() || rightBool.isTrue()) {
        return rightBool;
    }
    // NULL  False -> NULL
    // NULL  NULL  -> NULL
    return NValue::getNullValue(VALUE_TYPE_BOOLEAN);
}

}
#endif
