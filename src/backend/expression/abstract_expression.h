//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_expression.h
//
// Identification: src/backend/expression/abstract_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "backend/common/assert.h"
#include "backend/common/abstract_tuple.h"
#include "backend/common/printable.h"

#include "postgres.h"
#include "common/fe_memutils.h"

namespace peloton {

namespace executor {
class ExecutorContext;
}

namespace expression {

//===----------------------------------------------------------------------===//
// AbstractExpression
//
// Predicate objects for filtering tuples during query execution.
// These objects are stored in query plans and passed to Storage Access Manager.
//
// An expression usually has a longer life cycle than an execution, because,
// for example, it can be cached and reused for several executions of the same
// query template. Moreover, those executions can run simultaneously.
// So, an expression should not store per-execution information in its states.
// An expression tree (along with the plan node tree containing it) should
// remain constant and read-only during an execution.
//===----------------------------------------------------------------------===//

class AbstractExpression : public Printable {
 public:
  // destroy this node and all children
  virtual ~AbstractExpression();

  virtual Value Evaluate(const AbstractTuple *tuple1,
                         const AbstractTuple *tuple2,
                         executor::ExecutorContext *context) const = 0;

  /** return true if self or descendent should be substitute()'d */
  virtual bool HasParameter() const;

  /** accessors */
  ExpressionType GetExpressionType() const { return m_type; }

  ValueType GetValueType() const { return m_valueType; }

  const AbstractExpression *GetLeft() const { return m_left; }

  const AbstractExpression *GetRight() const { return m_right; }

  // Debugging methods - some various ways to create a string
  //     describing the expression tree
  std::string Debug() const;
  std::string Debug(bool traverse) const;
  std::string Debug(const std::string &spacer) const;
  virtual std::string DebugInfo(const std::string &spacer) const = 0;

  // Get a string representation for debugging
  const std::string GetInfo() const;

  virtual AbstractExpression *Copy() const = 0;

  inline AbstractExpression *CopyUtil(
      const AbstractExpression *expression) const {
    return (expression == nullptr) ? nullptr : expression->Copy();
  }

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  // Each sub-class will have to implement this function
  // After the implementation for each sub-class, we should set it to pure
  // virtual
  //===--------------------------------------------------------------------===//
  virtual bool SerializeTo(SerializeOutput &output) const {
    ASSERT(&output != nullptr);
    return false;
  }
  virtual bool DeserializeFrom(SerializeInputBE &input) {
    ASSERT(&input != nullptr);
    return false;
  }

  virtual int SerializeSize() { return 0; }

 protected:
  AbstractExpression(ExpressionType expr_type, ValueType type);
  AbstractExpression(ExpressionType expr_type, ValueType type,
                     AbstractExpression *left, AbstractExpression *right);

  ExpressionType m_type;
  ValueType m_valueType;

  AbstractExpression *m_left = nullptr;
  AbstractExpression *m_right = nullptr;

  bool m_hasParameter;

 private:
  bool InitParamShortCircuits();
};

}  // End expression namespace
}  // End peloton namespace
