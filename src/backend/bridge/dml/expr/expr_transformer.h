//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// expr_transformer.h
//
// Identification: src/backend/bridge/dml/expr/expr_transformer.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/expression/abstract_expression.h"

#include "postgres.h"
#include "nodes/execnodes.h"

namespace peloton {
namespace bridge {

/**
 * @brief Helper class to transform a Postgres Expr tree to Peloton Expr tree
 */
class ExprTransformer {
 public:
  ExprTransformer(const ExprTransformer &) = delete;
  ExprTransformer &operator=(const ExprTransformer &) = delete;
  ExprTransformer(const ExprTransformer &&) = delete;
  ExprTransformer &operator=(const ExprTransformer &&) = delete;

  static expression::AbstractExpression *TransformExpr(
      const ExprState *expr_state);
  static bool CleanExprTree(expression::AbstractExpression* root);
 private:
  /*
   * This set of TransformYYY methods should transform an PG ExprState tree
   * rooted at a ExprState pointing to a YYY Expr node.
   * A YYY Expr node should have a nodeTag of T_YYY.
   */

  static expression::AbstractExpression *TransformConst(const ExprState *es);
  static expression::AbstractExpression *TransformOp(const ExprState *es);
  static expression::AbstractExpression *TransformScalarArrayOp(const ExprState *es); //added by michael for IN operator
  static expression::AbstractExpression *TransformVar(const ExprState *es);
  static expression::AbstractExpression *TransformBool(const ExprState *es);
  static expression::AbstractExpression *TransformParam(const ExprState *es);
  static expression::AbstractExpression *TransformRelabelType(
      const ExprState *es);
  static expression::AbstractExpression *TransformFunc(const ExprState *es);
  static expression::AbstractExpression *TransformAggRef(const ExprState *es);

  static expression::AbstractExpression *TransformList(
      const ExprState *es, ExpressionType et = EXPRESSION_TYPE_CONJUNCTION_AND);

  /* Utilities */
  static expression::AbstractExpression*
  ReMapPgFunc(Oid pg_func_id, List* args);
};

}  // namespace bridge
}  // namespace peloton
