/*-------------------------------------------------------------------------
 *
 * expr_transformer.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "nodes/pprint.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "bridge/bridge.h"
#include "executor/executor.h"
#include "parser/parsetree.h"

#include "expr_transformer.h"


#include "backend/bridge/tuple_transformer.h"
#include "backend/common/logger.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/expression/expression_util.h"

namespace peloton {
namespace bridge {

void ExprTransformer::PrintPostgressExprTree(const ExprState* expr_state, std::string prefix) {
  auto tag = nodeTag(expr_state->expr);
  // Base case
}

/**
 * @brief Transform a ExprState tree (Postgres) to a AbstractExpression tree (Peloton) recursively.
 */
expression::AbstractExpression* ExprTransformer::TransformExpr(
    const ExprState* expr_state) {

  if(!expr_state)
    return nullptr;

  expression::AbstractExpression* peloton_expr;

  /*
   * NOTICE:
   * In Postgres, Expr and ExprState is NOT one-to-one.
   * Be careful of the Expr tag and ExprState type.
   */
  switch(nodeTag(expr_state->expr)){
    case T_Const:
      peloton_expr = TransformConstant(reinterpret_cast<const ExprState*>(expr_state));
      break;

    default:
      LOG_ERROR("Unsupported Postgres Expr type: %u\n", nodeTag(expr_state->expr));
  }




  return peloton_expr;
}

bool ExprTransformer::CleanExprTree(
    expression::AbstractExpression* root) {

  // AbstractExpression's destructor already handles deleting children
  delete root;

  return true;
}

expression::AbstractExpression* ExprTransformer::TransformConstant(
    const ExprState* expr_state) {

  auto const_expr = reinterpret_cast<Const*>(expr_state->expr);

  if(!const_expr->constbyval)
    LOG_ERROR("Sorry, we don't handle by-reference constant values currently.\n");

  Value value;

  if(const_expr->constisnull){ // Constant is null
    value = ValueFactory::GetNullValue();
  }
  else {  // non null
    value = TupleTransformer::DatumGetValue(const_expr->constvalue, const_expr->consttype);
  }

  // A Const Expr has no children.
  return expression::ConstantValueFactory(value);
}

} /* namespace bridge */
} /* namespace peloton */
