#pragma once

#include "backend/common/serializer.h"
#include "backend/common/value_vector.h"

#include "backend/expression/abstract_expression.h"

#include <string>

namespace nstore {
namespace expression {

class ConjunctionAnd;
class ConjunctionOr;

//===--------------------------------------------------------------------===//
// Conjunction Expression
//===--------------------------------------------------------------------===//

template <typename C>
class ConjunctionExpression : public AbstractExpression
{
 public:
  ConjunctionExpression(ExpressionType type,
                        AbstractExpression *left,
                        AbstractExpression *right)
 : AbstractExpression(type, left, right) {
    this->m_left = left;
    this->m_right = right;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2) const;

  std::string DebugInfo(const std::string &spacer) const {
    std::string retval;
    if(m_left != nullptr)
      retval = m_left->DebugInfo(spacer);
    retval += spacer + "ConjunctionExpression\n" + spacer;
    if(m_right != nullptr)
      retval = m_right->DebugInfo(spacer);
    return retval;
  }

  AbstractExpression *m_left;
  AbstractExpression *m_right;
};

template<> inline Value
ConjunctionExpression<ConjunctionAnd>::Evaluate(const AbstractTuple *tuple1,
                                                const AbstractTuple *tuple2) const {
  return m_left->Evaluate(tuple1, tuple2).OpAnd(m_right->Evaluate(tuple1, tuple2));
}

template<> inline Value
ConjunctionExpression<ConjunctionOr>::Evaluate(const AbstractTuple *tuple1,
                                               const AbstractTuple *tuple2) const {
  return m_left->Evaluate(tuple1, tuple2).OpOr(m_right->Evaluate(tuple1, tuple2));
}

} // End expression namespace
} // End nstore namespace
