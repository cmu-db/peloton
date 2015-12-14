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

    Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                   executor::ExecutorContext *context) const {
        assert (m_left);
        Value operand = m_left->Evaluate(tuple1, tuple2, context);
        // NOT TRUE.Is FALSE
        if (operand.IsTrue()) {
            return Value::GetFalse();
        }
        // NOT FALSE.Is TRUE
        if (operand.IsFalse()) {
            return Value::GetTrue();
        }
        // NOT NULL.Is NULL
        return operand;
    }

    std::string DebugInfo(const std::string &spacer) const {
        return (spacer + "OperatorNotExpression");
    }
};

class OperatorIsNullExpression : public AbstractExpression {
  public:
    OperatorIsNullExpression(AbstractExpression *left)
        : AbstractExpression(EXPRESSION_TYPE_OPERATOR_IS_NULL) {
            m_left = left;
    };

   Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                  executor::ExecutorContext *context) const {
       assert(m_left);
       Value tmp = m_left->Evaluate(tuple1, tuple2, context);
       if (tmp.IsNull()) {
           return Value::GetTrue();
       }
       else {
           return Value::GetFalse();
       }
   }

   std::string DebugInfo(const std::string &spacer) const {
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

    Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                   executor::ExecutorContext *context) const {
        assert (m_left);
        return m_left->Evaluate(tuple1, tuple2, context).CastAs(m_targetType);
    }

    std::string DebugInfo(const std::string &spacer) const {
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

    Value Evaluate(__attribute__((unused)) const AbstractTuple *tuple1,
                   __attribute__((unused)) const AbstractTuple *tuple2,
                   __attribute__((unused)) executor::ExecutorContext *context) const {
        throw Exception("OperatorAlternativeExpression::Evaluate function has no implementation.");
    }

    std::string DebugInfo(const std::string &spacer) const {
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

    Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                   executor::ExecutorContext *context) const {
        assert (m_left);
        assert (m_right);
        Value thenClause = m_left->Evaluate(tuple1, tuple2, context);

        if (thenClause.IsTrue()) {
            return m_right->GetLeft()->Evaluate(tuple1, tuple2, context).CastAs(m_returnType);
        } else {
            return m_right->GetRight()->Evaluate(tuple1, tuple2, context).CastAs(m_returnType);
        }
    }

    std::string DebugInfo(const std::string &spacer) const {
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
    Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
             executor::ExecutorContext *context) const
    {
        assert(m_left);
        assert(m_right);
        return oper.op(m_left->Evaluate(tuple1, tuple2, context),
                       m_right->Evaluate(tuple1, tuple2, context));
    }

    std::string DebugInfo(const std::string &spacer) const {
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
    Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
             executor::ExecutorContext *context) const;

    std::string DebugInfo(const std::string &spacer) const {
        return (spacer + "OperatorE.IstsExpression");
    }
};

}  // End expression namespace
}  // End peloton namespace
