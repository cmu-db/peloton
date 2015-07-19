#pragma once

#include "backend/common/value.h"
#include "backend/common/serializer.h"
#include "backend/common/value_vector.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/parameter_value_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"

#include <string>

namespace peloton {
namespace expression {

//===--------------------------------------------------------------------===//
// Comparison Expression
//===--------------------------------------------------------------------===//

class CmpEq {
 public:
  inline Value cmp(Value l, Value r) const { return l.OpEquals(r);}
};

class CmpNe {
 public:
  inline Value cmp(Value l, Value r) const { return l.OpNotEquals(r);}
};

class CmpLt {
 public:
  inline Value cmp(Value l, Value r) const { return l.OpLessThan(r);}
};

class CmpGt {
 public:
  inline Value cmp(Value l, Value r) const { return l.OpGreaterThan(r);}
};

class CmpLte {
 public:
  inline Value cmp(Value l, Value r) const { return l.OpLessThanOrEqual(r);}
};

class CmpGte {
 public:
  inline Value cmp(Value l, Value r) const { return l.OpGreaterThanOrEqual(r);}
};

template <typename C>
class ComparisonExpression : public AbstractExpression {

 public:
  ComparisonExpression(ExpressionType type,
                       AbstractExpression *left,
                       AbstractExpression *right)
 : AbstractExpression(type, left, right){
    this->m_left = left;
    this->m_right = right;
  };

  inline Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                        ExpressionContext* ec) const {
    assert(m_left != NULL);
    assert(m_right != NULL);

    return this->compare.cmp(this->m_left->Evaluate(tuple1, tuple2, ec),
                             this->m_right->Evaluate(tuple1, tuple2, ec));
  }

  std::string DebugInfo(const std::string &spacer) const {
    std::string retval;
    if(m_left != nullptr)
      retval = m_left->DebugInfo(spacer);
    retval += spacer + "ComparisonExpression\n" + spacer;
    if(m_right != nullptr)
      retval = m_right->DebugInfo(spacer);
    return retval;
  }

 private:
  AbstractExpression *m_left;
  AbstractExpression *m_right;
  C compare;
};

template <typename C, typename L, typename R>
class InlinedComparisonExpression : public AbstractExpression {
 public:

  InlinedComparisonExpression(ExpressionType type,
                              AbstractExpression *left,
                              AbstractExpression *right)
 : AbstractExpression(type, left, right) {
    this->left_expr = left;
    this->m_leftTyped = dynamic_cast<L*>(left);
    this->right_expr = right;
    this->m_rightTyped = dynamic_cast<R*>(right);

    assert (m_leftTyped != NULL);
    assert (m_rightTyped != NULL);
  };

  inline Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2, ExpressionContext* ec ) const {
    return this->compare.cmp(this->left_expr->Evaluate(tuple1, tuple2, ec), this->right_expr->Evaluate(tuple1, tuple2, ec));
  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OptimizedInlinedComparisonExpression\n");
  }

 private:
  L *m_leftTyped;
  R *m_rightTyped;
  C compare;
};

} // End expression namespace
} // End peloton namespace
