//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_expression.h
//
// Identification: src/backend/expression/abstract_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "backend/common/value.h"
#include "backend/common/abstract_tuple.h"
#include "backend/common/types.h"
#include "backend/common/planner_dom_value.h"

#include "boost/shared_ptr.hpp"

namespace peloton {
namespace expression {

//===--------------------------------------------------------------------===//
// AbstractExpression
//===--------------------------------------------------------------------===//

/**
 * Predicate objects for filtering tuples during query execution.
 * These objects are stored in query plans and passed to Storage Access Manager.
 *
 * An expression usually has a longer life cycle than an execution, because,
 * for example, it can be cached and reused for several executions of the same
 * query template.
 * Moreover, those executions can run simultaneously.
 * So, an expression should not store per-execution information in its states.
 * An expression tree (along with the plan node tree containing it) should
 * remain
 * constant and read-only during an execution.
 */

class AbstractExpression {
 public:
  // destroy this node and all children
  virtual ~AbstractExpression();

  virtual Value eval(const AbstractTuple *tuple1 = NULL,
                     const AbstractTuple *tuple2 = NULL,
                     executor::ExecutorContext *context /* = nullptr */) const = 0;

  /** return true if self or descendent should be substitute()'d */
  virtual bool hasParameter() const;

  /* debugging methods - some various ways to create a sring
       describing the expression tree */
  std::string debug() const;
  std::string debug(bool traverse) const;
  std::string debug(const std::string &spacer) const;
  virtual std::string debugInfo(const std::string &spacer) const = 0;

  /* serialization methods. expression are serialized in java and
       deserialized in the execution engine during startup. */


  /** accessors */
  ExpressionType getExpressionType() const {
    return m_type;
  }

  ValueType getValueType() const
  {
    return m_valueType;
  }

  int getValueSize() const
  {
    return m_valueSize;
  }

  bool getInBytes() const
  {
    return m_inBytes;
  }

  // These should really be part of the constructor, but plumbing
  // the type and size args through the whole of the expression world is
  // not something I'm doing right now.
  void setValueType(ValueType type)
  {
    m_valueType = type;
  }

  void setInBytes(bool bytes)
  {
    m_inBytes = bytes;
  }

  void setValueSize(int size)
  {
    m_valueSize = size;
  }

  const AbstractExpression *getLeft() const {
    return m_left;
  }

  const AbstractExpression *getRight() const {
    return m_right;
  }

 protected:
  AbstractExpression();
  AbstractExpression(ExpressionType type);
  AbstractExpression(ExpressionType type,
                     AbstractExpression *left,
                     AbstractExpression *right);

 private:
  bool initParamShortCircuits();

 protected:

  AbstractExpression *m_left, *m_right;

  ExpressionType m_type;

  bool m_hasParameter;

  ValueType m_valueType = VALUE_TYPE_INVALID;

  int m_valueSize = 0;

  bool m_inBytes = false;

};

}  // End expression namespace
}  // End peloton namespace
