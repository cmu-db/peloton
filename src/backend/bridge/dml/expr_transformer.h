/*-------------------------------------------------------------------------
 *
 * expr_transformer.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "nodes/execnodes.h"

#include "backend/expression/abstract_expression.h"


namespace peloton {
namespace bridge {


/**
 * @brief Helper class to transform a Postgres Expr tree to Peloton Expr tree
 */
class ExprTransformer {

 public:
  ExprTransformer(const ExprTransformer &) = delete;
  ExprTransformer& operator=(const ExprTransformer &) = delete;
  ExprTransformer(const ExprTransformer &&) = delete;
  ExprTransformer& operator=(const ExprTransformer &&) = delete;

  static void PrintPostgressExprTree(const ExprState* expr_state, std::string prefix = "");

  static expression::AbstractExpression* TransformExpr(const ExprState* expr_state);

  static bool CleanExprTree(expression::AbstractExpression* root);

 private:

  static expression::AbstractExpression* TransformConstant(const ExprState* es);
  static expression::AbstractExpression* TransformOp(const ExprState* es);
  static expression::AbstractExpression* TransformVar(const ExprState* es);
  static expression::AbstractExpression* TransformBool(const ExprState* es);

  static expression::AbstractExpression* TransformList(const ExprState* es,
                                                       ExpressionType et = EXPRESSION_TYPE_CONJUNCTION_AND);



};

} /* namespace bridge */
} /* namespace peloton */

