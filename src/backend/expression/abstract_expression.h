/*-------------------------------------------------------------------------
 *
 * abstract_expression.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/expression/abstract_expression.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <string>
#include <vector>

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_vector.h"
#include "backend/common/abstract_tuple.h"
#include "backend/executor/executor_context.h"

#include <json_spirit.h>

namespace peloton {
namespace expression {

class SerializeInput;
class SerializeOutput;
class AbstractExpression;

//===--------------------------------------------------------------------===//
// AbstractExpression
//===--------------------------------------------------------------------===//

/**
 * Predicate objects for filtering tuples during query execution.
 * These objects are stored in query plans and passed to Storage Access Manager.
 *
 * An expression usually has a longer life cycle than an execution, because,
 * for example, it can be cached and reused for several executions of the same
 *query template.
 * Moreover, those executions can run simultaneously.
 * So, an expression should not store per-execution information in its states.
 * An expression tree (along with the plan node tree containing it) should
 *remain
 * constant and read-only during an execution.
 * Substituted values are passed as parameter to Evaluate().
 * - Qiang, 7/17/15
 */
class AbstractExpression {
 public:
  // destroy this node and all children
  virtual ~AbstractExpression();

  /**
   * @brief Evaluate the expression tree recursively and return a Value.
   * @param tuple1 The left tuple.
   * @param tuple2 The right tuple.
   * @param exprcontex  The expression context that is passed through the tree.
   * It is used when needed and no cost otherwise.
   */
  virtual Value Evaluate(
      const AbstractTuple *tuple1, const AbstractTuple *tuple2,
      executor::ExecutorContext *context /* = nullptr */) const = 0;

  // set parameter values for this node and its descendants
  virtual void Substitute(const ValueArray &params);

  // return true if self or descendant should be substitute()'d
  virtual bool HasParameter() const;

  /// Get a string representation for debugging
  friend std::ostream &operator<<(std::ostream &os,
                                  const AbstractExpression &expr);

  std::string Debug() const;
  std::string Debug(bool traverse) const;
  std::string Debug(const std::string &spacer) const;
  virtual std::string DebugInfo(const std::string &spacer = "->") const = 0;

  // create an expression tree. call this once with the input
  // stream positioned at the root expression node
  static AbstractExpression *CreateExpressionTree(json_spirit::Object &obj);

  // Accessors

  ExpressionType GetExpressionType() const { return expr_type; }

  const AbstractExpression *GetLeft() const { return left_expr; }

  const AbstractExpression *GetRight() const { return right_expr; }

  AbstractExpression *GetExpression() const { return expr; }

  const char *GetName() const { return name; }

  const char *GetColumn() const { return column; }

  const char *GetAlias() const { return alias; }

  // Parser expression

  int ival = 0;
  AbstractExpression *expr = nullptr;

  char *name = nullptr;
  char *column = nullptr;
  char *alias = nullptr;

  bool distinct = false;

 protected:
  AbstractExpression();

  AbstractExpression(ExpressionType type);

  AbstractExpression(ExpressionType type, AbstractExpression *left,
                     AbstractExpression *right);

  //===--------------------------------------------------------------------===//
  // Data Members
  //===--------------------------------------------------------------------===//

  // children
  AbstractExpression *left_expr = nullptr, *right_expr = nullptr;

  ExpressionType expr_type;

  // true if we need to substitute ?
  bool has_parameter;

 private:
  static AbstractExpression *CreateExpressionTreeRecurse(
      json_spirit::Object &obj);

  bool InitParamShortCircuits();
};

}  // End expression namespace
}  // End peloton namespace
