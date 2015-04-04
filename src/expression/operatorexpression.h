

#ifndef HSTOREOPREATOREXPRESSION_H
#define HSTOREOPERATOREXPRESSION_H

#include "common/common.h"
#include "common/serializeio.h"
#include "common/valuevector.h"

#include "expressions/abstractexpression.h"

#include <string>

namespace voltdb {


/*
 * Unary operators. Currently just operator NOT.
 */

class OperatorNotExpression : public AbstractExpression {
public:
    OperatorNotExpression(AbstractExpression *left)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_NOT) {
        m_left = left;
    };

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        assert (m_left);
        return m_left->eval(tuple1, tuple2).op_negate();
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
    inline NValue op(NValue left, NValue right) const { return left.op_add(right); }
};

class OpMinus {
public:
    inline NValue op(NValue left, NValue right) const { return left.op_subtract(right); }
};

class OpMultiply {
public:
    inline NValue op(NValue left, NValue right) const { return left.op_multiply(right); }
};

class OpDivide {
public:
    inline NValue op(NValue left, NValue right) const { return left.op_divide(right); }
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

    NValue
    eval(const TableTuple *tuple1, const TableTuple *tuple2) const
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


}
#endif
