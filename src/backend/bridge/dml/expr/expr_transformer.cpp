/*-------------------------------------------------------------------------
 *
 * expr_transformer.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include <unordered_map>

#include "nodes/pprint.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "parser/parsetree.h"

#include "backend/bridge/dml/tuple/tuple_transformer.h"
#include "backend/bridge/dml/expr/pg_func_map.h"
#include "backend/bridge/dml/expr/expr_transformer.h"
#include "backend/bridge/dml/executor/plan_executor.h"
#include "backend/common/logger.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/expression/expression_util.h"

namespace peloton {
namespace bridge {

expression::AbstractExpression* ReMapPgFunc(Oid pg_func_id,
                                            expression::AbstractExpression* lc,
                                            expression::AbstractExpression* rc);

extern std::unordered_map<Oid, peloton::ExpressionType> pg_func_map;

void ExprTransformer::PrintPostgressExprTree(const ExprState* expr_state,
                                             std::string prefix) {
  auto tag = nodeTag(expr_state->expr);

  /* TODO Not complete.
   * Not all ExprState has a child / children list,
   * so it would take some multiplexing to print it recursively here.
   */
  LOG_INFO("%u ", tag);
}

/**
 * @brief Transform a ExprState tree (Postgres) to a AbstractExpression tree
 * (Peloton) recursively.
 */
expression::AbstractExpression* ExprTransformer::TransformExpr(
    const ExprState* expr_state) {
  if (!expr_state) return nullptr;

  /* Special case:
   * Input is a list of expressions.
   * Transform it to a conjunction tree.
   */
  if (expr_state->type == T_List) {
    return TransformList(expr_state);
  }

  expression::AbstractExpression* peloton_expr = nullptr;

  switch (nodeTag(expr_state->expr)) {
    case T_Const:
      peloton_expr = TransformConst(expr_state);
      break;

    case T_OpExpr:
      peloton_expr = TransformOp(expr_state);
      break;

    case T_Var:
      peloton_expr = TransformVar(expr_state);
      break;

    case T_BoolExpr:
      peloton_expr = TransformBool(expr_state);
      break;

    case T_Param:
      peloton_expr = TransformParam(expr_state);
      break;

    case T_RelabelType:
      peloton_expr = TransformRelabelType(expr_state);
      break;

    case T_FuncExpr:
      peloton_expr = TransformFunc(expr_state);
      break;

    default:
      LOG_ERROR("Unsupported Postgres Expr type: %u (see 'nodes.h')\n",
                nodeTag(expr_state->expr));
  }

  return peloton_expr;
}

bool ExprTransformer::CleanExprTree(expression::AbstractExpression* root) {
  // AbstractExpression's destructor already handles deleting children
  delete root;

  return true;
}

expression::AbstractExpression* ExprTransformer::TransformConst(
    const ExprState* es) {
  auto const_expr = reinterpret_cast<const Const*>(es->expr);

  Value value;

  if (const_expr->constisnull) {  // Constant is null
    value = ValueFactory::GetNullValue();
  } else if (const_expr->constbyval) {  // non null
    value = TupleTransformer::GetValue(const_expr->constvalue,
                                       const_expr->consttype);
  } else if (const_expr->constlen == -1) {
    LOG_TRACE("Probably handing a string constant \n");
    value = TupleTransformer::GetValue(const_expr->constvalue,
                                       const_expr->consttype);
  } else {
    LOG_ERROR(
        "Unknown Const profile: constlen = %d , constbyval = %d, constvalue = "
        "%lu \n",
        const_expr->constlen, const_expr->constbyval,
        (long unsigned)const_expr->constvalue);
  }

  LOG_TRACE("Const : ");
  std::cout << value << std::endl;

  // A Const Expr has no children.
  return expression::ConstantValueFactory(value);
}

expression::AbstractExpression* ExprTransformer::TransformOp(
    const ExprState* es) {
  LOG_TRACE("Transform Op \n");

  auto op_expr = reinterpret_cast<const OpExpr*>(es->expr);
  auto func_state = reinterpret_cast<const FuncExprState*>(es);

  assert(op_expr->opfuncid !=
         0);  // Hopefully it has been filled in by PG planner
  assert(list_length(func_state->args) <=
         2);  // Hopefully it has at most two parameters

  expression::AbstractExpression* lc = nullptr;
  expression::AbstractExpression* rc = nullptr;

  // Add function arguments as children
  int i = 0;
  ListCell* arg;
  foreach (arg, func_state->args) {
    ExprState* argstate = (ExprState*)lfirst(arg);

    if (i == 0)
      lc = TransformExpr(argstate);
    else if (i == 1)
      rc = TransformExpr(argstate);
    else
      break;  // skip >2 arguments

    i++;
  }

  return ReMapPgFunc(op_expr->opfuncid, lc, rc);
}

expression::AbstractExpression* ExprTransformer::TransformVar(
    const ExprState* es) {
  // Var expr only needs default ES
  auto var_expr = reinterpret_cast<const Var*>(es->expr);

  assert(var_expr->varattno != InvalidAttrNumber);

  oid_t tuple_idx =
      (var_expr->varno == INNER_VAR
           ? 1
           : 0);  // Seems reasonable, c.f. ExecEvalScalarVarFast()
  oid_t value_idx =
      static_cast<oid_t>(var_expr->varattno - 1);  // Damnit attno is 1-index

  LOG_TRACE("tuple_idx = %u , value_idx = %u \n", tuple_idx, value_idx);

  // TupleValue expr has no children.
  return expression::TupleValueFactory(tuple_idx, value_idx);
}

expression::AbstractExpression* ExprTransformer::TransformBool(
    const ExprState* es) {
  auto bool_expr = reinterpret_cast<const BoolExpr*>(es->expr);
  auto bool_state = reinterpret_cast<const BoolExprState*>(es);

  auto bool_op = bool_expr->boolop;

  /*
   * AND and OR can take >=2 arguments,
   * while NOT should take only one.
   */
  assert(bool_state->args);
  assert(bool_op != NOT_EXPR || list_length(bool_state->args) == 1);
  assert(bool_op == NOT_EXPR || list_length(bool_state->args) >= 2);

  auto args = bool_state->args;

  switch (bool_op) {
    case AND_EXPR:
      LOG_TRACE("Bool AND list \n");
      return TransformList(reinterpret_cast<const ExprState*>(args),
                           EXPRESSION_TYPE_CONJUNCTION_AND);

    case OR_EXPR:
      LOG_TRACE("Bool OR list \n");
      return TransformList(reinterpret_cast<const ExprState*>(args),
                           EXPRESSION_TYPE_CONJUNCTION_OR);

    case NOT_EXPR: {
      LOG_TRACE("Bool NOT \n");
      auto child_es =
          reinterpret_cast<const ExprState*>(lfirst(list_head(args)));
      auto child = TransformExpr(child_es);
      return expression::OperatorFactory(EXPRESSION_TYPE_OPERATOR_NOT, child,
                                         nullptr);
    }

    default:
      LOG_ERROR("Unrecognized BoolExpr : %u", bool_op);
  }

  return nullptr;
}

expression::AbstractExpression* ExprTransformer::TransformParam(
    const ExprState* es) {
  auto param_expr = reinterpret_cast<const Param*>(es->expr);

  switch (param_expr->paramkind) {
    case PARAM_EXTERN:
      LOG_TRACE("Handle EXTREN PARAM");
      return expression::ParameterValueFactory(param_expr->paramid -
                                               1);  // 1 indexed
      break;
    default:
      LOG_ERROR("Unrecognized param kind %d", param_expr->paramkind);
      break;
  }

  return nullptr;
}

expression::AbstractExpression* ExprTransformer::TransformRelabelType(
    const ExprState* es) {
  auto state = reinterpret_cast<const GenericExprState*>(es);
  auto expr = reinterpret_cast<const RelabelType*>(es->expr);
  auto child_state = state->arg;

  assert(expr->relabelformat == COERCE_IMPLICIT_CAST);

  LOG_TRACE("Handle relabel as %d", expr->resulttype);
  expression::AbstractExpression* child =
      ExprTransformer::TransformExpr(child_state);

  PostgresValueType type = static_cast<PostgresValueType>(expr->resulttype);

  return expression::CastFactory(type, child);
}

expression::AbstractExpression* ExprTransformer::TransformFunc(
    const ExprState* es) {
  // auto state = reinterpret_cast<const FuncExprState*>(es);
  auto expr = reinterpret_cast<const FuncExpr*>(es->expr);
  // auto expr_args = reinterpret_cast<const ExprState*>(state->args);

  assert(expr->xpr.type == T_FuncExpr);

  LOG_TRACE("Return type: %d, isReturn %d, Coercion: %d", expr->funcresulttype,
            expr->funcretset, expr->funcformat);
  // expression::AbstractExpression *args =
  // ExprTransformer::TransformExpr(expr_args);
  // LOG_TRACE("args : %s", args->DebugInfo(" ").c_str());

  // TODO: not implemented yet

  return nullptr;
}

expression::AbstractExpression* ExprTransformer::TransformList(
    const ExprState* es, ExpressionType et) {
  assert(et == EXPRESSION_TYPE_CONJUNCTION_AND ||
         et == EXPRESSION_TYPE_CONJUNCTION_OR);

  const List* list = reinterpret_cast<const List*>(es);
  ListCell* l;
  int length = list_length(list);
  assert(length > 0);
  LOG_TRACE("Expression List of length %d", length);
  std::list<expression::AbstractExpression*>
      exprs;  // a list of AND'ed expressions

  foreach (l, list) {
    const ExprState* expr_state = reinterpret_cast<const ExprState*>(lfirst(l));
    exprs.push_back(ExprTransformer::TransformExpr(expr_state));
  }

  return expression::ConjunctionFactory(et, exprs);
}

/**
 * @brief Helper function: re-map Postgres's builtin function Oid
 * to proper expression type in Peloton
 *
 * @param pg_func_id  PG Function Id used to lookup function in \b
 *fmrg_builtin[]
 * (see Postgres source file 'fmgrtab.cpp')
 * @param lc          Left child.
 * @param rc          Right child.
 * @return            Corresponding expression tree in peloton.
 */
expression::AbstractExpression* ReMapPgFunc(
    Oid func_id, expression::AbstractExpression* lc,
    expression::AbstractExpression* rc) {
  auto itr = pg_func_map.find(func_id);

  if (itr == pg_func_map.end()) {
    LOG_ERROR("Unsupported PG Function ID : %u (check fmgrtab.cpp)\n", func_id);
    return nullptr;
  }

  auto pl_expr_type = itr->second;

  switch (pl_expr_type) {
    case EXPRESSION_TYPE_COMPARE_EQ:
    case EXPRESSION_TYPE_COMPARE_NE:
    case EXPRESSION_TYPE_COMPARE_GT:
    case EXPRESSION_TYPE_COMPARE_LT:
    case EXPRESSION_TYPE_COMPARE_GTE:
    case EXPRESSION_TYPE_COMPARE_LTE:
      return expression::ComparisonFactory(pl_expr_type, lc, rc);

    case EXPRESSION_TYPE_OPERATOR_PLUS:
    case EXPRESSION_TYPE_OPERATOR_MINUS:
    case EXPRESSION_TYPE_OPERATOR_MULTIPLY:
    case EXPRESSION_TYPE_OPERATOR_DIVIDE:
      return expression::OperatorFactory(pl_expr_type, lc, rc);

    default:
      LOG_ERROR(
          "This Peloton ExpressionType is in our map but not transformed here "
          ": %u",
          pl_expr_type);
  }

  return nullptr;
}

}  // namespace bridge
}  // namespace peloton
