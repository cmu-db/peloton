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

#pragma once

#include "backend/common/serializer.h"

#include "backend/expression/abstract_expression.h"

#include <string>

namespace peloton {
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

    Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                   executor::ExecutorContext *context) const;

    std::string DebugInfo(const std::string &spacer) const {
        return (spacer + "ConjunctionExpression\n");
    }

    AbstractExpression *m_left;
    AbstractExpression *m_right;
};

template<> inline Value
ConjunctionExpression<ConjunctionAnd>::Evaluate(const AbstractTuple *tuple1,
                                                const AbstractTuple *tuple2,
                                                executor::ExecutorContext *context) const
{
    Value leftBool = m_left->Evaluate(tuple1, tuple2, context);
    // False False -> False
    // False True  -> False
    // False NULL  -> False
    if (leftBool.IsFalse()) {
        return leftBool;
    }
    Value rightBool = m_right->Evaluate(tuple1, tuple2, context);
    // True  False -> False
    // True  True  -> True
    // True  NULL  -> NULL
    // NULL  False -> False
    if (leftBool.IsTrue() || rightBool.IsFalse()) {
        return rightBool;
    }
    // NULL  True  -> NULL
    // NULL  NULL  -> NULL
    return Value::GetNullValue(VALUE_TYPE_BOOLEAN);
}

template<> inline Value
ConjunctionExpression<ConjunctionOr>::Evaluate(const AbstractTuple *tuple1,
                                               const AbstractTuple *tuple2,
                                               executor::ExecutorContext *context) const
{
    Value leftBool = m_left->Evaluate(tuple1, tuple2, context);
    // True True  -> True
    // True False -> True
    // True NULL  -> True
    if (leftBool.IsTrue()) {
        return leftBool;
    }
    Value rightBool = m_right->Evaluate(tuple1, tuple2, context);
    // False True  -> True
    // False False -> False
    // False NULL  -> NULL
    // NULL  True  -> True
    if (leftBool.IsFalse() || rightBool.IsTrue()) {
        return rightBool;
    }
    // NULL  False -> NULL
    // NULL  NULL  -> NULL
    return Value::GetNullValue(VALUE_TYPE_BOOLEAN);
}

}  // End expression namespace
}  // End peloton namespace
