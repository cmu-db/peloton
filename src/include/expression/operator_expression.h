//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_expression.h
//
// Identification: src/expression/operator_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/serializer.h"

#include "expression/abstract_expression.h"

#include <string>

namespace peloton {
namespace expression {

/*
 * Unary operators. (NOT and IS_NULL)
 */

class OperatorUnaryNotExpression : public AbstractExpression {
 public:
  OperatorUnaryNotExpression(AbstractExpression *left)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_NOT, VALUE_TYPE_BOOLEAN,
                           left, nullptr) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    PL_ASSERT(GetLeft() != nullptr);
    Value operand = GetLeft()->Evaluate(tuple1, tuple2, context);
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

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorNotExpression");
  }

  AbstractExpression *Copy() const override {
    return new OperatorUnaryNotExpression(CopyUtil(GetLeft()));
  }
};

class OperatorIsNullExpression : public AbstractExpression {
 public:
  OperatorIsNullExpression(AbstractExpression *left)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_IS_NULL, VALUE_TYPE_BOOLEAN,
                           left, nullptr) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    PL_ASSERT(GetLeft() != nullptr);
    Value tmp = GetLeft()->Evaluate(tuple1, tuple2, context);
    if (tmp.IsNull()) {
      return Value::GetTrue();
    } else {
      return Value::GetFalse();
    }
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorIsNullExpression");
  }

  AbstractExpression *Copy() const override {
    return new OperatorIsNullExpression(CopyUtil(m_left));
  }
};

class OperatorCastExpression : public AbstractExpression {
 public:
  OperatorCastExpression(ValueType vt, AbstractExpression *left)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_CAST, vt, left, nullptr) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    PL_ASSERT(GetLeft() != nullptr);
    return GetLeft()->Evaluate(tuple1, tuple2, context).CastAs(GetValueType());
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "CastExpression");
  }

  AbstractExpression *Copy() const override {
    return new OperatorCastExpression(GetValueType(), CopyUtil(GetLeft()));
  }
};

class OperatorUnaryMinusExpression : public AbstractExpression {
 public:
  OperatorUnaryMinusExpression(AbstractExpression *left)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_UNARY_MINUS,
                           left->GetValueType(), left, nullptr) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    PL_ASSERT(GetLeft() != nullptr);
    Value operand = GetLeft()->Evaluate(tuple1, tuple2, context);
    // NOT TRUE.Is FALSE
    return Value::GetUnaryMinus(operand);
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorNotExpression");
  }

  AbstractExpression *Copy() const override {
    return new OperatorUnaryMinusExpression(CopyUtil(GetLeft()));
  }
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
  inline Value op(Value left, Value right) const {
    return left.OpSubtract(right);
  }
};

class OpMultiply {
 public:
  inline Value op(Value left, Value right) const {
    return left.OpMultiply(right);
  }
};

class OpDivide {
 public:
  inline Value op(Value left, Value right) const {
    return left.OpDivide(right);
  }
};

class OpMod {
 public:
  inline Value op(Value left, Value right) const { return left.OpMod(right); }
};

/*
 * Expressions templated on binary operator types
 */

template <typename OPER>
class OperatorExpression : public AbstractExpression {
 public:
  OperatorExpression(ExpressionType type, ValueType value_type,
                     AbstractExpression *left, AbstractExpression *right)
      : AbstractExpression(type, value_type, left, right) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    PL_ASSERT(GetLeft() != nullptr);
    PL_ASSERT(GetRight() != nullptr);
    return oper.op(m_left->Evaluate(tuple1, tuple2, context),
                   m_right->Evaluate(tuple1, tuple2, context));
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OptimizedOperatorExpression");
  }

  AbstractExpression *Copy() const override {
    // TODO: How about OPER oper?
    return new OperatorExpression<OPER>(GetExpressionType(), GetValueType(),
                                        CopyUtil(GetLeft()),
                                        CopyUtil(GetRight()));
  }

 private:
  OPER oper;
};

class OperatorExistsExpression : public AbstractExpression {
 public:
  OperatorExistsExpression(AbstractExpression *left)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_EXISTS, VALUE_TYPE_BOOLEAN,
                           left, nullptr) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override ;

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorE.IstsExpression");
  }

  AbstractExpression *Copy() const override {
    return new OperatorExistsExpression(CopyUtil(GetLeft()));
  }
};

}  // End expression namespace
}  // End peloton namespace
