#pragma once

#include "common/value.h"
#include "common/serializer.h"
#include "common/value_vector.h"

#include "expression/abstract_expression.h"
#include "expression/parameter_value_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"

#include <string>

namespace nstore {
namespace expression {

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
 : AbstractExpression(type, left, right)
 {
    this->m_left = left;
    this->m_right = right;
 };

  inline Value eval(const storage::Tuple *tuple1, const storage::Tuple *tuple2) const {
    //VOLT_TRACE("eval %s. left %s, right %s. ret=%s",
    //           typeid(this->compare).name(), typeid(*(this->m_left)).name(),
    //           typeid(*(this->m_right)).name(),
    //           this->compare.cmp(this->m_left->eval(tuple1, tuple2),
    //                             this->m_right->eval(tuple1, tuple2)).isTrue()
    //                             ? "TRUE" : "FALSE");

    assert(m_left != NULL);
    assert(m_right != NULL);

    return this->compare.cmp(
        this->m_left->eval(tuple1, tuple2),
        this->m_right->eval(tuple1, tuple2));
  }

  std::string debugInfo(const std::string &spacer) const {
    return (spacer + "ComparisonExpression\n");
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
 : AbstractExpression(type, left, right)
 {
    this->m_left = left;
    this->m_leftTyped = dynamic_cast<L*>(left);
    this->m_right = right;
    this->m_rightTyped = dynamic_cast<R*>(right);

    assert (m_leftTyped != NULL);
    assert (m_rightTyped != NULL);
 };

  inline Value eval(const storage::Tuple *tuple1, const storage::Tuple *tuple2 ) const {
    return this->compare.cmp(
        this->m_left->eval(tuple1, tuple2),
        this->m_right->eval(tuple1, tuple2));
  }

  std::string debugInfo(const std::string &spacer) const {
    return (spacer + "OptimizedInlinedComparisonExpression\n");
  }

 private:
  L *m_leftTyped;
  R *m_rightTyped;
  C compare;
};

} // End expression namespace
} // End nstore namespace
