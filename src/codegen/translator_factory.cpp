//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// translator_factory.cpp
//
// Identification: src/codegen/translator_factory.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/translator_factory.h"

#include "codegen/expression/arithmetic_translator.h"
#include "codegen/expression/case_translator.h"
#include "codegen/expression/comparison_translator.h"
#include "codegen/expression/conjunction_translator.h"
#include "codegen/expression/constant_translator.h"
#include "codegen/expression/function_translator.h"
#include "codegen/expression/negation_translator.h"
#include "codegen/expression/null_check_translator.h"
#include "codegen/expression/parameter_translator.h"
#include "codegen/expression/tuple_value_translator.h"
#include "codegen/operator/block_nested_loop_join_translator.h"
#include "codegen/operator/delete_translator.h"
#include "codegen/operator/global_group_by_translator.h"
#include "codegen/operator/hash_group_by_translator.h"
#include "codegen/operator/hash_join_translator.h"
#include "codegen/operator/hash_translator.h"
#include "codegen/operator/insert_translator.h"
#include "codegen/operator/order_by_translator.h"
#include "codegen/operator/projection_translator.h"
#include "codegen/operator/table_scan_translator.h"
#include "codegen/operator/update_translator.h"
#include "expression/aggregate_expression.h"
#include "expression/case_expression.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/function_expression.h"
#include "expression/operator_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/aggregate_plan.h"
#include "planner/delete_plan.h"
#include "planner/hash_join_plan.h"
#include "planner/hash_plan.h"
#include "planner/insert_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/order_by_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/update_plan.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Create a translator for the given operator
//===----------------------------------------------------------------------===//
std::unique_ptr<OperatorTranslator> TranslatorFactory::CreateTranslator(
    const planner::AbstractPlan &plan_node, CompilationContext &context,
    Pipeline &pipeline) const {
  OperatorTranslator *translator = nullptr;
  switch (plan_node.GetPlanNodeType()) {
    case PlanNodeType::SEQSCAN: {
      auto &scan = static_cast<const planner::SeqScanPlan &>(plan_node);
      translator = new TableScanTranslator(scan, context, pipeline);
      break;
    }
    case PlanNodeType::PROJECTION: {
      auto &projection =
          static_cast<const planner::ProjectionPlan &>(plan_node);
      translator = new ProjectionTranslator(projection, context, pipeline);
      break;
    }
    case PlanNodeType::HASHJOIN: {
      auto &join = static_cast<const planner::HashJoinPlan &>(plan_node);
      translator = new HashJoinTranslator(join, context, pipeline);
      break;
    }
    case PlanNodeType::NESTLOOP: {
      auto &join = static_cast<const planner::NestedLoopJoinPlan &>(plan_node);
      translator = new BlockNestedLoopJoinTranslator(join, context, pipeline);
      break;
    }
    case PlanNodeType::HASH: {
      auto &hash = static_cast<const planner::HashPlan &>(plan_node);
      translator = new HashTranslator(hash, context, pipeline);
      break;
    }
    case PlanNodeType::AGGREGATE_V2: {
      const auto &aggregate_plan =
          static_cast<const planner::AggregatePlan &>(plan_node);
      // An aggregation without any grouping clause is simpler to handle. All
      // other aggregations are handled using a hash-group-by for now.
      // TODO: Implement other (non hash) group-by algorithms
      if (aggregate_plan.IsGlobal()) {
        translator =
            new GlobalGroupByTranslator(aggregate_plan, context, pipeline);
      } else {
        translator =
            new HashGroupByTranslator(aggregate_plan, context, pipeline);
      }
      break;
    }
    case PlanNodeType::ORDERBY: {
      auto &order_by = static_cast<const planner::OrderByPlan &>(plan_node);
      translator = new OrderByTranslator(order_by, context, pipeline);
      break;
    }
    case PlanNodeType::DELETE: {
      auto &delete_plan = static_cast<const planner::DeletePlan &>(plan_node);
      translator = new DeleteTranslator(delete_plan, context, pipeline);
      break;
    }
    case PlanNodeType::INSERT: {
      auto &insert_plan = static_cast<const planner::InsertPlan &>(plan_node);
      translator = new InsertTranslator(insert_plan, context, pipeline);
      break;
    }
    case PlanNodeType::UPDATE: {
      auto &update_plan = static_cast<const planner::UpdatePlan &>(plan_node);
      translator = new UpdateTranslator(update_plan, context, pipeline);
      break;
    }
    default: {
      throw Exception{"We don't have a translator for plan node type: " +
                      PlanNodeTypeToString(plan_node.GetPlanNodeType())};
    }
  }
  PL_ASSERT(translator != nullptr);
  return std::unique_ptr<OperatorTranslator>{translator};
}

//===----------------------------------------------------------------------===//
// Create a translator for the given expression
//===----------------------------------------------------------------------===//
std::unique_ptr<ExpressionTranslator> TranslatorFactory::CreateTranslator(
    const expression::AbstractExpression &exp,
    CompilationContext &context) const {
  ExpressionTranslator *translator = nullptr;
  switch (exp.GetExpressionType()) {
    case ExpressionType::VALUE_PARAMETER: {
      auto &param_exp =
          static_cast<const expression::ParameterValueExpression &>(exp);
      translator = new ParameterTranslator(param_exp, context);
      break;
    }
    case ExpressionType::VALUE_CONSTANT: {
      auto &const_exp =
          static_cast<const expression::ConstantValueExpression &>(exp);
      translator = new ConstantTranslator(const_exp, context);
      break;
    }
    case ExpressionType::VALUE_TUPLE: {
      auto &tve_exp =
          static_cast<const expression::TupleValueExpression &>(exp);
      translator = new TupleValueTranslator(tve_exp, context);
      break;
    }
    case ExpressionType::COMPARE_EQUAL:
    case ExpressionType::COMPARE_NOTEQUAL:
    case ExpressionType::COMPARE_LESSTHAN:
    case ExpressionType::COMPARE_GREATERTHAN:
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
    case ExpressionType::COMPARE_LIKE: {
      const auto &cmp_exp =
          static_cast<const expression::ComparisonExpression &>(exp);
      translator = new ComparisonTranslator(cmp_exp, context);
      break;
    }
    case ExpressionType::CONJUNCTION_AND:
    case ExpressionType::CONJUNCTION_OR: {
      const auto &conjunction_exp =
          static_cast<const expression::ConjunctionExpression &>(exp);
      translator = new ConjunctionTranslator(conjunction_exp, context);
      break;
    }
    case ExpressionType::OPERATOR_PLUS:
    case ExpressionType::OPERATOR_MINUS:
    case ExpressionType::OPERATOR_MULTIPLY:
    case ExpressionType::OPERATOR_DIVIDE:
    case ExpressionType::OPERATOR_MOD: {
      auto &arithmetic_exp =
          static_cast<const expression::OperatorExpression &>(exp);
      translator = new ArithmeticTranslator(arithmetic_exp, context);
      break;
    }
    case ExpressionType::OPERATOR_UNARY_MINUS: {
      auto &negation_exp =
          static_cast<const expression::OperatorUnaryMinusExpression &>(exp);
      translator = new NegationTranslator(negation_exp, context);
      break;
    }
    case ExpressionType::OPERATOR_IS_NULL:
    case ExpressionType::OPERATOR_IS_NOT_NULL: {
      auto &null_check_exp =
          static_cast<const expression::OperatorExpression &>(exp);
      translator = new NullCheckTranslator(null_check_exp, context);
      break;
    }
    case ExpressionType::OPERATOR_CASE_EXPR: {
      auto &case_exp = static_cast<const expression::CaseExpression &>(exp);
      translator = new CaseTranslator(case_exp, context);
      break;
    }
    case ExpressionType::FUNCTION: {
      auto &func_exp = static_cast<const expression::FunctionExpression &>(exp);
      translator = new FunctionTranslator(func_exp, context);
      break;
    }
    default: {
      throw Exception{"We don't have a translator for expression type: " +
                      ExpressionTypeToString(exp.GetExpressionType())};
    }
  }
  PL_ASSERT(translator != nullptr);
  return std::unique_ptr<ExpressionTranslator>{translator};
}

}  // namespace codegen
}  // namespace peloton
