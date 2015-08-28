//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// operator_expression.h
//
// Identification: src/backend/expression/operator_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/serializer.h"
#include "backend/common/value_vector.h"

#include "backend/expression/abstract_expression.h"

#include <string>
#include <cassert>

namespace peloton {
namespace expression {

/*
 * Unary operators. (NOT and IS_NULL)
 */

class OperatorNotExpression : public AbstractExpression {
public:
    OperatorNotExpression(AbstractExpression *left)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_NOT) {
        m_left = left;
    };

    Value eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        assert (m_left);
        Value operand = m_left->eval(tuple1, tuple2);
        // NOT TRUE is FALSE
        if (operand.isTrue()) {
            return Value::getFalse();
        }
        // NOT FALSE is TRUE
        if (operand.isFalse()) {
            return Value::getTrue();
        }
        // NOT NULL is NULL
        return operand;
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "OperatorNotExpression");
    }
};

class OperatorIsNullExpression : public AbstractExpression {
  public:
    OperatorIsNullExpression(AbstractExpression *left)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_IS_NULL) {
            m_left = left;
    };

   Value eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
       assert(m_left);
       Value tmp = m_left->eval(tuple1, tuple2);
       if (tmp.isNull()) {
           return Value::getTrue();
       }
       else {
           return Value::getFalse();
       }
   }

   std::string debugInfo(const std::string &spacer) const {
       return (spacer + "OperatorIsNullExpression");
   }
};

class OperatorCastExpression : public AbstractExpression {
public:
    OperatorCastExpression(ValueType vt, AbstractExpression *left)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_CAST)
        , m_targetType(vt)
    {
        m_left = left;
    };

    Value eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        assert (m_left);
        return m_left->eval(tuple1, tuple2).castAs(m_targetType);
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "CastExpression");
    }
private:
    ValueType m_targetType;
};

class OperatorAlternativeExpression : public AbstractExpression {
public:
    OperatorAlternativeExpression(AbstractExpression *left, AbstractExpression *right)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_ALTERNATIVE, left, right)
    {
        assert (m_left);
        assert (m_right);
    };

    Value eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        throwFatalException("OperatorAlternativeExpression::eval function has no implementation.");
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "Operator ALTERNATIVE Expression");
    }

};

class OperatorCaseWhenExpression : public AbstractExpression {
public:
    OperatorCaseWhenExpression(ValueType vt, AbstractExpression *left, OperatorAlternativeExpression *right)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_CASE_WHEN, left, right)
        , m_returnType(vt)
    {
    };

    Value eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        assert (m_left);
        assert (m_right);
        Value thenClause = m_left->eval(tuple1, tuple2);

        if (thenClause.isTrue()) {
            return m_right->getLeft()->eval(tuple1, tuple2).castAs(m_returnType);
        } else {
            return m_right->getRight()->eval(tuple1, tuple2).castAs(m_returnType);
        }
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "Operator CASE WHEN Expression");
    }
private:
    ValueType m_returnType;
};



/*
 * Binary operators.
 */

class OpPlus {
public:
    inline Value op(Value left, Value right) const { return left.op_add(right); }
};

class OpMinus {
public:
    inline Value op(Value left, Value right) const { return left.op_subtract(right); }
};

class OpMultiply {
public:
    inline Value op(Value left, Value right) const { return left.op_multiply(right); }
};

class OpDivide {
public:
    inline Value op(Value left, Value right) const { return left.op_divide(right); }
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

class OperatorExistsExpression : public AbstractExpression {
  public:
    OperatorExistsExpression(AbstractExpression *left)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_EXISTS, left, NULL)
    {
    }

    Value
    eval(const TableTuple *tuple1, const TableTuple *tuple2) const;

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "OperatorExistsExpression");
    }
};

}  // End expression namespace
}  // End peloton namespace
