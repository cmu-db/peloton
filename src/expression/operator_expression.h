#pragma once

#include "common/serializer.h"
#include "common/value_vector.h"

#include "expression/abstract_expression.h"

#include <string>

namespace nstore {
namespace expression {

/*
 * Unary operators. Currently just operator NOT.
 */

class OperatorNotExpression : public AbstractExpression {
public:
    OperatorNotExpression(AbstractExpression *left)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_NOT) {
        m_left = left;
    };

    Value eval(const storage::Tuple *tuple1, const storage::Tuple *tuple2) const {
        assert (m_left);
        return m_left->eval(tuple1, tuple2).OpNegate();
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "OptimizedOperatorNotExpression");
    }

private:
    AbstractExpression *m_left;
};



/*
 * Binary operators.
 */

class OpPlus {
public:
    inline Value op(Value left, Value right) const { return left.OpAdd(right); }
};

class OpMinus {
public:
    inline Value op(Value left, Value right) const { return left.OpSubtract(right); }
};

class OpMultiply {
public:
    inline Value op(Value left, Value right) const { return left.OpMultiply(right); }
};

class OpDivide {
public:
    inline Value op(Value left, Value right) const { return left.OpDivide(right); }
};


/*
 * Expressions templated on binary operator types
 */

template <typename OPER>
class OperatorExpression : public AbstractExpression {
  public:
    OperatorExpression(ExpressionType type,
                                AbstractExpression *left,
                                AbstractExpression *right)
        : AbstractExpression(type, left, right)
    {
    }

    Value
    eval(const storage::Tuple *tuple1, const storage::Tuple *tuple2) const
    {
        assert(m_left);
        assert(m_right);
        return oper.op(m_left->eval(tuple1, tuple2),
                       m_right->eval(tuple1, tuple2));
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "OptimizedOperatorExpression");
    }
private:
    OPER oper;
};

} // End expression namespace
} // End nstore namespace



