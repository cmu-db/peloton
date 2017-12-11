//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer.cpp
//
// Identification: src/optimizer/optimizer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "optimizer/optimizer.h"

#include "catalog/column_catalog.h"
#include "catalog/manager.h"
#include "catalog/table_catalog.h"

#include "optimizer/binding.h"
#include "optimizer/cost_and_stats_calculator.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/properties.h"
#include "optimizer/property_enforcer.h"
#include "optimizer/query_to_operator_transformer.h"
#include "optimizer/input_column_deriver.h"
#include "optimizer/plan_generator.h"
#include "optimizer/rule.h"
#include "optimizer/rule_impls.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/optimize_context.h"
#include "parser/create_statement.h"

#include "planner/analyze_plan.h"
#include "planner/create_function_plan.h"
#include "planner/create_plan.h"
#include "planner/drop_plan.h"
#include "planner/order_by_plan.h"
#include "planner/populate_index_plan.h"
#include "planner/projection_plan.h"
#include "planner/seq_scan_plan.h"

#include "storage/data_table.h"

#include "binder/bind_node_visitor.h"

using std::vector;
using std::unordered_map;
using std::shared_ptr;
using std::unique_ptr;
using std::move;
using std::pair;
using std::make_shared;

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Optimizer
//===--------------------------------------------------------------------===//
Optimizer::Optimizer() {}

void Optimizer::OptimizeLoop(int root_group_id,
                             std::shared_ptr<PropertySet> required_props) {
  std::shared_ptr<OptimizeContext> root_context =
      std::make_shared<OptimizeContext>(&metadata_, required_props);
  auto task_stack =
      std::unique_ptr<OptimizerTaskStack>(new OptimizerTaskStack());
  metadata_.SetTaskPool(task_stack.get());

  // Perform optimization after the rewrite
  task_stack->Push(new OptimizeGroup(metadata_.memo.GetGroupByID(root_group_id),
                                     root_context));
  // Perform rewrite first
  task_stack->Push(new TopDownRewrite(root_group_id, root_context,
                                      RewriteRuleSetName::PREDICATE_PUSH_DOWN));

  task_stack->Push(new BottomUpRewrite(root_group_id, root_context,
                                      RewriteRuleSetName::UNNEST_SUBQUERY, false));

  // TODO: Add timer for early stop
  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->execute();
  }
}

shared_ptr<planner::AbstractPlan> Optimizer::BuildPelotonPlanTree(
    const unique_ptr<parser::SQLStatementList> &parse_tree_list,
    const std::string default_database_name,
    concurrency::TransactionContext *txn) {
  // Base Case
  if (parse_tree_list->GetStatements().size() == 0) return nullptr;

  unique_ptr<planner::AbstractPlan> child_plan = nullptr;

  auto parse_tree = parse_tree_list->GetStatements().at(0).get();

  // Run binder
  auto bind_node_visitor =
      make_shared<binder::BindNodeVisitor>(txn, default_database_name);
  bind_node_visitor->BindNameToNode(parse_tree);

  metadata_.catalog_cache = &txn->catalog_cache;
  // Handle ddl statement
  bool is_ddl_stmt;
  auto ddl_plan = HandleDDLStatement(parse_tree, is_ddl_stmt, txn);
  if (is_ddl_stmt) {
    return move(ddl_plan);
  }

  // Generate initial operator tree from query tree
  shared_ptr<GroupExpression> gexpr = InsertQueryTree(parse_tree, txn);
  GroupID root_id = gexpr->GetGroupID();
  // Get the physical properties the final plan must output
  auto query_info = GetQueryInfo(parse_tree);

  OptimizeLoop(root_id, query_info.physical_props);

  try {
    auto best_plan = ChooseBestPlan(root_id, query_info.physical_props,
                                    query_info.output_exprs);
    if (best_plan == nullptr) return nullptr;
    // Reset memo after finishing the optimization
    Reset();
    //  return shared_ptr<planner::AbstractPlan>(best_plan.release());
    return move(best_plan);
  } catch (Exception &e) {
    Reset();
    throw e;
  }
}

void Optimizer::Reset() { metadata_ = OptimizerMetadata(); }

unique_ptr<planner::AbstractPlan> Optimizer::HandleDDLStatement(
    parser::SQLStatement *tree, bool &is_ddl_stmt,
    concurrency::TransactionContext *txn) {
  unique_ptr<planner::AbstractPlan> ddl_plan = nullptr;
  is_ddl_stmt = true;
  auto stmt_type = tree->GetType();
  switch (stmt_type) {
    case StatementType::DROP: {
      LOG_TRACE("Adding Drop plan...");
      unique_ptr<planner::AbstractPlan> drop_plan(
          new planner::DropPlan((parser::DropStatement *)tree));
      ddl_plan = move(drop_plan);
      break;
    }

    case StatementType::CREATE: {
      LOG_TRACE("Adding Create plan...");

      // This is adapted from the simple optimizer
      auto create_plan =
          new planner::CreatePlan((parser::CreateStatement *)tree);
      std::unique_ptr<planner::AbstractPlan> child_CreatePlan(create_plan);
      ddl_plan = move(child_CreatePlan);

      if (create_plan->GetCreateType() == peloton::CreateType::INDEX) {
        auto create_stmt = (parser::CreateStatement *)tree;
        auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
            create_stmt->GetDatabaseName(), create_stmt->GetTableName(), txn);
        std::vector<oid_t> column_ids;
        // use catalog object instead of schema to acquire metadata
        auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
            create_stmt->GetDatabaseName(), create_stmt->GetTableName(), txn);
        for (auto column_name : create_plan->GetIndexAttributes()) {
          auto column_object = table_object->GetColumnObject(column_name);
          // Check if column is missing
          if (column_object == nullptr)
            throw CatalogException(
                "Some columns are missing when create index " +
                std::string(create_stmt->index_name));
          oid_t col_pos = column_object->GetColumnId();
          column_ids.push_back(col_pos);
        }
        // Create a plan to retrieve data
        std::unique_ptr<planner::SeqScanPlan> child_SeqScanPlan(
            new planner::SeqScanPlan(target_table, nullptr, column_ids, false));

        child_SeqScanPlan->AddChild(std::move(ddl_plan));
        ddl_plan = std::move(child_SeqScanPlan);
        // Create a plan to add data to index
        std::unique_ptr<planner::AbstractPlan> child_PopulateIndexPlan(
            new planner::PopulateIndexPlan(target_table, column_ids));
        child_PopulateIndexPlan->AddChild(std::move(ddl_plan));
        create_plan->SetKeyAttrs(column_ids);
        ddl_plan = std::move(child_PopulateIndexPlan);
      }
      break;
    }
    case StatementType::TRANSACTION: {
      break;
    }
    case StatementType::CREATE_FUNC: {
      LOG_TRACE("Adding Create function plan...");
      unique_ptr<planner::AbstractPlan> create_func_plan(
          new planner::CreateFunctionPlan(
              (parser::CreateFunctionStatement *)tree));
      ddl_plan = move(create_func_plan);
    } break;
    case StatementType::ANALYZE: {
      LOG_TRACE("Adding Analyze plan...");
      unique_ptr<planner::AbstractPlan> analyze_plan(new planner::AnalyzePlan(
          static_cast<parser::AnalyzeStatement *>(tree), txn));
      ddl_plan = move(analyze_plan);
      break;
    }
    case StatementType::COPY: {
      LOG_TRACE("Adding Copy plan...");
      parser::CopyStatement *copy_parse_tree =
          static_cast<parser::CopyStatement *>(tree);
      ddl_plan = util::CreateCopyPlan(copy_parse_tree);
      break;
    }
    default:
      is_ddl_stmt = false;
  }
  return ddl_plan;
}

shared_ptr<GroupExpression> Optimizer::InsertQueryTree(
    parser::SQLStatement *tree, concurrency::TransactionContext *txn) {
  QueryToOperatorTransformer converter(txn);
  shared_ptr<OperatorExpression> initial =
      converter.ConvertToOpExpression(tree);
  shared_ptr<GroupExpression> gexpr;
  metadata_.RecordTransformedExpression(initial, gexpr);
  return gexpr;
}

QueryInfo Optimizer::GetQueryInfo(parser::SQLStatement *tree) {
  auto GetQueryInfoHelper = [](
      std::vector<unique_ptr<expression::AbstractExpression>> &select_list,
      std::unique_ptr<parser::OrderDescription> &order_info,
      std::vector<expression::AbstractExpression *> &output_exprs,
      std::shared_ptr<PropertySet> &physical_props) {
    // Extract output column
    for (auto &expr : select_list) output_exprs.push_back(expr.get());

    // Extract sort property
    if (order_info != nullptr) {
      std::vector<expression::AbstractExpression *> sort_exprs;
      std::vector<bool> sort_ascending;
      for (auto &expr : order_info->exprs) {
        sort_exprs.push_back(expr.get());
      }
      for (auto &type : order_info->types) {
        sort_ascending.push_back(type == parser::kOrderAsc);
      }
      if (!sort_exprs.empty())
        physical_props->AddProperty(
            std::make_shared<PropertySort>(sort_exprs, sort_ascending));
    }
  };

  std::vector<expression::AbstractExpression *> output_exprs;
  std::shared_ptr<PropertySet> physical_props = std::make_shared<PropertySet>();
  switch (tree->GetType()) {
    case StatementType::SELECT: {
      auto select = reinterpret_cast<parser::SelectStatement *>(tree);
      GetQueryInfoHelper(select->select_list, select->order, output_exprs,
                         physical_props);
      break;
    }
    case StatementType::INSERT: {
      auto insert = reinterpret_cast<parser::InsertStatement *>(tree);
      if (insert->select != nullptr)
        GetQueryInfoHelper(insert->select->select_list, insert->select->order,
                           output_exprs, physical_props);
      break;
    }
    default:;
  }

  return QueryInfo(output_exprs, physical_props);
}

unique_ptr<planner::AbstractPlan> Optimizer::ChooseBestPlan(
    GroupID id, std::shared_ptr<PropertySet> required_props,
    std::vector<expression::AbstractExpression *> required_cols) {
  Group *group = metadata_.memo.GetGroupByID(id);
  LOG_TRACE("Choosing with property : %s", required_props->ToString().c_str());
  auto gexpr = group->GetBestExpression(required_props);

  LOG_TRACE("Choosing best plan for group %d with op %s", gexpr->GetGroupID(),
            gexpr->Op().name().c_str());

  vector<GroupID> child_groups = gexpr->GetChildGroupIDs();
  auto required_input_props = gexpr->GetInputProperties(required_props);
  PL_ASSERT(required_input_props.size() == child_groups.size());
  // Firstly derive input/output columns
  InputColumnDeriver deriver;
  auto output_input_cols_pair = deriver.DeriveInputColumns(
      gexpr, required_props, required_cols, &metadata_.memo);
  auto &output_cols = output_input_cols_pair.first;
  auto &input_cols = output_input_cols_pair.second;
  PL_ASSERT(input_cols.size() == required_input_props.size());

  // Derive chidren plans first because they are useful in the derivation of
  // root plan. Also keep propagate expression to column offset mapping
  vector<unique_ptr<planner::AbstractPlan>> children_plans;
  vector<ExprMap> children_expr_map;
  for (size_t i = 0; i < child_groups.size(); ++i) {
    ExprMap child_expr_map;
    for (unsigned offset = 0; offset < input_cols[i].size(); ++offset) {
      PL_ASSERT(input_cols[i][offset] != nullptr);
      child_expr_map[input_cols[i][offset]] = offset;
    }
    auto child_plan =
        ChooseBestPlan(child_groups[i], required_input_props[i], input_cols[i]);
    children_expr_map.push_back(move(child_expr_map));
    PL_ASSERT(child_plan != nullptr);
    children_plans.push_back(move(child_plan));
  }

  // Derive root plan
  shared_ptr<OperatorExpression> op =
      make_shared<OperatorExpression>(gexpr->Op());

  PlanGenerator generator;
  auto plan = generator.ConvertOpExpression(op, required_props, required_cols,
                                            output_cols, children_plans,
                                            children_expr_map);

  LOG_TRACE("Finish Choosing best plan for group %d", id);
  return plan;
}
}  // namespace optimizer
}  // namespace peloton
