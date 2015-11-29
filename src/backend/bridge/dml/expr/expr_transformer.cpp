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
#include "nodes/execnodes.h"
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
#include "backend/expression/cast_expression.h"

namespace peloton {
namespace bridge {


/**
 * @brief Transform a ExprState tree (Postgres) to a AbstractExpression tree
 * (Peloton) recursively.
 * @return  The transformed expression tree. NULL if input is empty.
 */
expression::AbstractExpression* ExprTransformer::TransformExpr(
    const ExprState* expr_state) {
  if (nullptr == expr_state) {
    LOG_TRACE("Null expression");
    return nullptr;
  }

  expression::AbstractExpression* peloton_expr = nullptr;

  /* Special case:
   * Input is a list of expressions.
   * Transform it to a conjunction tree.
   */
  if (expr_state->type == T_List) {
    peloton_expr = TransformList(expr_state);
    return peloton_expr;
  }

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

    case T_Aggref:
      peloton_expr = TransformAggRef(expr_state);
      break;

    default:
      LOG_ERROR("Unsupported Postgres Expr type: %u (see 'nodes.h')\n",
                nodeTag(expr_state->expr))
      ;
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
        (long unsigned )const_expr->constvalue);
  }

  // A Const Expr has no children.
  auto rv = expression::ConstantValueFactory(value);
  value.Free();
  return rv;
}

expression::AbstractExpression* ExprTransformer::TransformOp(
    const ExprState* es) {
  LOG_TRACE("Transform Op \n");

  auto op_expr = reinterpret_cast<const OpExpr*>(es->expr);
  auto func_state = reinterpret_cast<const FuncExprState*>(es);

  assert(op_expr->opfuncid != 0);  // Hopefully it has been filled in by PG planner

  auto pg_func_id = op_expr->opfuncid;

  return ReMapPgFunc(pg_func_id, func_state->args);
}

expression::AbstractExpression* ExprTransformer::TransformFunc(
    const ExprState* es) {
  auto fn_es = reinterpret_cast<const FuncExprState*>(es);
  auto fn_expr = reinterpret_cast<const FuncExpr*>(es->expr);

  assert(fn_expr->xpr.type == T_FuncExpr);

  auto pg_func_id = fn_expr->funcid;
  auto rettype = fn_expr->funcresulttype;

  LOG_TRACE("PG Func oid : %u , return type : %u \n", pg_func_id, rettype);
  LOG_TRACE("PG funcid in planstate : %u\n", fn_es->func.fn_oid);

  auto retval = ReMapPgFunc(pg_func_id, fn_es->args);


  // FIXME It will generate incorrect results.
  if(!retval){
    LOG_ERROR("Unknown function. By-pass it for now. (May be incorrect.");
    assert(list_length(fn_es->args) > 0);

    ExprState* first_child = (ExprState*) lfirst(list_head(fn_es->args));
    return TransformExpr(first_child);
  }

  if (retval->GetExpressionType() == EXPRESSION_TYPE_CAST) {
    expression::CastExpression *cast_expr = reinterpret_cast<expression::CastExpression*>(retval);
    ExprState* first_child = (ExprState*) lfirst(list_head(fn_es->args));
    cast_expr->SetChild(TransformExpr(first_child));
    PostgresValueType type = static_cast<PostgresValueType>(rettype);
    cast_expr->SetResultType(type);
    return cast_expr;
  }

  return retval;
}

expression::AbstractExpression* ExprTransformer::TransformVar(
    const ExprState* es) {
  // Var expr only needs default ES
  auto var_expr = reinterpret_cast<const Var*>(es->expr);

  oid_t tuple_idx = (var_expr->varno == INNER_VAR ? 1 : 0);  // Seems reasonable, c.f. ExecEvalScalarVarFast()

  /*
   * Special case: an varattno of zero in PG
   * means return the whole row.
   * We don't want that, just return null.
   */
  if (!AttributeNumberIsValid(
      var_expr->varattno) || !AttrNumberIsForUserDefinedAttr(var_expr->varattno)) {
    return nullptr;
  }

  oid_t value_idx = static_cast<oid_t>(AttrNumberGetAttrOffset(
      var_expr->varattno));

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
      LOG_ERROR("Unrecognized BoolExpr : %u", bool_op)
      ;
  }

  return nullptr;
}

expression::AbstractExpression* ExprTransformer::TransformParam(
    const ExprState* es) {
  auto param_expr = reinterpret_cast<const Param*>(es->expr);

  switch (param_expr->paramkind) {
    case PARAM_EXTERN:
      LOG_TRACE("Handle EXTREN PARAM");
      return expression::ParameterValueFactory(param_expr->paramid - 1);  // 1 indexed
      break;
    default:
      LOG_ERROR("Unrecognized param kind %d", param_expr->paramkind)
      ;
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
  expression::AbstractExpression* child = ExprTransformer::TransformExpr(
      child_state);

  PostgresValueType type = static_cast<PostgresValueType>(expr->resulttype);

  return expression::CastFactory(type, child);
}

expression::AbstractExpression* ExprTransformer::TransformAggRef(
    const ExprState* es) {
  auto aggref_state = reinterpret_cast<const AggrefExprState*>(es);

  assert(aggref_state->aggno >= 0);

  int value_idx = aggref_state->aggno;
  int tuple_idx = 1;

  // Raw aggregate values would be passed as the RIGHT tuple
  return expression::TupleValueFactory(tuple_idx, value_idx);

}

expression::AbstractExpression* ExprTransformer::TransformList(
    const ExprState* es, ExpressionType et) {
  assert(
      et == EXPRESSION_TYPE_CONJUNCTION_AND
          || et == EXPRESSION_TYPE_CONJUNCTION_OR);

  const List* list = reinterpret_cast<const List*>(es);
  ListCell* l;
  int length = list_length(list);
  if(length == 0)
    return nullptr;
  LOG_TRACE("Expression List of length %d", length);
  std::list<expression::AbstractExpression*> exprs;  // a list of AND'ed expressions

  foreach (l, list)
  {
    const ExprState* expr_state = reinterpret_cast<const ExprState*>(lfirst(l));
    exprs.push_back(ExprTransformer::TransformExpr(expr_state));
  }

  return expression::ConjunctionFactory(et, exprs);
}

/**
 * @brief Helper function: re-map Postgres's builtin function
 * to proper expression type in Peloton
 *
 * @param pg_func_id  PG Function Id used to lookup function in \b
 *fmrg_builtin[]
 * (see Postgres source file 'fmgrtab.cpp')
 * @param args  The argument list in PG ExprState
 * @return            Corresponding expression tree in peloton.
 */
expression::AbstractExpression*
ExprTransformer::ReMapPgFunc(Oid pg_func_id, List* args) {

  assert(pg_func_id > 0);


  // Perform lookup
  auto itr = kPgFuncMap.find(pg_func_id);

  if (itr == kPgFuncMap.end()) {
    LOG_ERROR("Unsupported PG Op Function ID : %u (check fmgrtab.cpp)\n",
              pg_func_id);
    return nullptr;
  }
  PltFuncMetaInfo func_meta = itr->second;

  if (func_meta.exprtype == EXPRESSION_TYPE_CAST) {
    // it is a cast, but we need casted type and the child expr.
    // so we just create an empty cast expr and return it.
    return expression::CastFactory();
  }

  assert(list_length(args) <= 2);  // Hopefully it has at most two parameters

  // Extract function arguments (at most two)
  expression::AbstractExpression* lc = nullptr;
  expression::AbstractExpression* rc = nullptr;
  int i = 0;
  ListCell* arg;
  foreach (arg, args)
  {
    ExprState* argstate = (ExprState*) lfirst(arg);

    if (i >= func_meta.nargs)
      break;
    if (i == 0)
      lc = TransformExpr(argstate);
    else if (i == 1)
      rc = TransformExpr(argstate);
    else break;

    i++;
  }

  // Construct the corresponding Peloton expression
  auto plt_exprtype = func_meta.exprtype;

  switch (plt_exprtype) {
    case EXPRESSION_TYPE_COMPARE_EQUAL:
    case EXPRESSION_TYPE_COMPARE_NOTEQUAL:
    case EXPRESSION_TYPE_COMPARE_GREATERTHAN:
    case EXPRESSION_TYPE_COMPARE_LESSTHAN:
    case EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO:
    case EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO:
      return expression::ComparisonFactory(plt_exprtype, lc, rc);

    case EXPRESSION_TYPE_OPERATOR_PLUS:
    case EXPRESSION_TYPE_OPERATOR_MINUS:
    case EXPRESSION_TYPE_OPERATOR_MULTIPLY:
    case EXPRESSION_TYPE_OPERATOR_DIVIDE:
      return expression::OperatorFactory(plt_exprtype, lc, rc);

    default:
      LOG_ERROR(
          "This Peloton ExpressionType is in our map but not transformed here "
          ": %u",
          plt_exprtype)
      ;
  }

  return nullptr;
}

}  // namespace bridge
}  // namespace peloton
