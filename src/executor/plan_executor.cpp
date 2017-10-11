//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_executor.cpp
//
// Identification: src/executor/plan_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/plan_executor.h"

#include "codegen/buffering_consumer.h"
#include "codegen/query_compiler.h"
#include "codegen/query.h"
#include "concurrency/transaction_manager_factory.h"
#include "common/logger.h"
#include "executor/executor_context.h"
#include "executor/executors.h"
#include "storage/tuple_iterator.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace executor {

executor::AbstractExecutor *BuildExecutorTree(executor::AbstractExecutor *root,
                                              const planner::AbstractPlan *plan,
                                              executor::ExecutorContext *executor_context);

void CleanExecutorTree(executor::AbstractExecutor *root);

/**
 * @brief Build a executor tree and execute it.
 * Use std::vector<type::Value> as params to make it more elegant for
 * networking
 * Before ExecutePlan, a node first receives value list, so we should pass
 * value list directly rather than passing Postgres's ParamListInfo
 * @return status of execution.
 */
void PlanExecutor::ExecutePlan(
    std::shared_ptr<planner::AbstractPlan> plan,
    concurrency::Transaction *txn, const std::vector<type::Value> &params,
    std::vector<StatementResult> &result,
    const std::vector<int> &result_format,
    executor::ExecuteResult &p_status) {
  PL_ASSERT(plan != nullptr && txn != nullptr);
  LOG_TRACE("PlanExecutor Start (Txn ID=%lu)", txn->GetTransactionId());

  result.clear();
  std::unique_ptr<executor::ExecutorContext> executor_context(
      new executor::ExecutorContext(txn, params));

  if (!settings::SettingsManager::GetBool(settings::SettingId::codegen)
      || !codegen::QueryCompiler::IsSupported(*plan)) {
    bool status;
    std::unique_ptr<executor::AbstractExecutor> executor_tree(
        BuildExecutorTree(nullptr, plan.get(), executor_context.get()));

    status = executor_tree->Init();
    if (status != true) {
      p_status.m_result = ResultType::FAILURE;
      p_status.m_result_slots = nullptr;
      CleanExecutorTree(executor_tree.get());
      return;
    }

    // Execute the tree until we get result tiles from root node
    while (status == true) {
      status = executor_tree->Execute();
      std::unique_ptr<executor::LogicalTile> tile(executor_tree->GetOutput());

      // Some executors don't return logical tiles (e.g., Update).
      if (tile.get() != nullptr) {
        LOG_TRACE("Final Answer: %s", tile->GetInfo().c_str());
        std::vector<std::vector<std::string>> tuples;
        tuples = tile->GetAllValuesAsStrings(result_format, false);

        // Construct the returned results
        for (auto &tuple : tuples) {
          for (unsigned int i = 0; i < tile->GetColumnCount(); i++) {
            auto res = StatementResult();
            PlanExecutor::copyFromTo(tuple[i], res.second);
            result.push_back(std::move(res));
            LOG_TRACE("column content: %s",
                      tuple[i].c_str() != nullptr ?  tuple[i].c_str() : "-emptry-");
          }
        }
      }
    }
    p_status.m_processed = executor_context->num_processed;
    p_status.m_result = ResultType::SUCCESS;
    p_status.m_result_slots = nullptr;
    CleanExecutorTree(executor_tree.get());
    return;
  }

  LOG_TRACE("Compiling and executing query ...");
  // Perform binding
  planner::BindingContext context;
  plan->PerformBinding(context);

  // Prepare output buffer
  std::vector<oid_t> columns;
  plan->GetOutputColumns(columns);
  codegen::BufferingConsumer consumer{columns, context};

  // Compile & execute the query
  codegen::QueryCompiler compiler;
  auto query = compiler.Compile(*plan, consumer);
  query->Execute(*txn, executor_context.get(),
                 reinterpret_cast<char *>(consumer.GetState()));

  // Iterate over results
  const auto &results = consumer.GetOutputTuples();
  for (const auto &tuple : results) {
    for (uint32_t i = 0; i < tuple.tuple_.size(); i++) {
      auto res = StatementResult();
      auto column_val = tuple.GetValue(i);
      auto str = column_val.IsNull() ? "" : column_val.ToString();
      PlanExecutor::copyFromTo(str, res.second);
      LOG_TRACE("column content: [%s]", str.c_str());
      result.push_back(std::move(res));
    }
  }
  p_status.m_processed = executor_context->num_processed;
  p_status.m_result = ResultType::SUCCESS;
  p_status.m_result_slots = nullptr;
  return;
}

// FIXME this function is here temporarily to support PelotonService
// which should be refactorized to use ExecutePlan() above
/**
 * @brief Build a executor tree and execute it.
 * Use std::vector<type::Value> as params to make it more elegant for
 * networking
 * Before ExecutePlan, a node first receives value list, so we should pass
 * value list directly rather than passing Postgres's ParamListInfo
 * @return number of executed tuples and logical_tile_list
 */
int PlanExecutor::ExecutePlan(const planner::AbstractPlan *plan,
                              const std::vector<type::Value> &params,
                              std::vector<std::unique_ptr<executor::LogicalTile>> &logical_tile_list) {
  PL_ASSERT(plan != nullptr);
  LOG_TRACE("PlanExecutor Start with transaction");

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  PL_ASSERT(txn);
  LOG_TRACE("Txn ID = %lu ", txn->GetTransactionId());

  std::unique_ptr<executor::ExecutorContext> executor_context(
      new executor::ExecutorContext(txn, params));
  std::unique_ptr<executor::AbstractExecutor> executor_tree(
      BuildExecutorTree(nullptr, plan, executor_context.get()));

  bool init_failure = false;
  bool status = executor_tree->Init();
  if (status == false) {
    init_failure = true;
    txn->SetResult(ResultType::FAILURE);
    goto cleanup;
  }
  LOG_TRACE("Running the executor tree");

  // Execute the tree until we get result tiles from root node
  while (status == true) {
    status = executor_tree->Execute();
    if (status == false)
      break;

    std::unique_ptr<executor::LogicalTile> logical_tile(
        executor_tree->GetOutput());
    logical_tile_list.push_back(std::move(logical_tile));
  }

  cleanup:
  LOG_TRACE("About to commit: init_failure: %d, status: %s", init_failure,
            ResultTypeToString(txn->GetResult()).c_str());

  // clean up executor tree
  CleanExecutorTree(executor_tree.get());

  // should we commit or abort ?
  if (init_failure == true) {
    auto status = txn->GetResult();
    switch (status) {
      case ResultType::SUCCESS:
        return executor_context->num_processed;
      case ResultType::FAILURE:
      default:
        return -1;
    }
  }
  return executor_context->num_processed;
}

/**
 * @brief Build the executor tree.
 * @param The current executor tree
 * @param The plan tree
 * @param Transation context
 * @return The updated executor tree.
 */
executor::AbstractExecutor *BuildExecutorTree(executor::AbstractExecutor *root,
                                              const planner::AbstractPlan *plan,
                                              executor::ExecutorContext *executor_context) {
  // Base case
  if (plan == nullptr) return root;

  executor::AbstractExecutor *child_executor = nullptr;

  auto plan_node_type = plan->GetPlanNodeType();
  switch (plan_node_type) {
    case PlanNodeType::INVALID:
      LOG_ERROR("Invalid plan node type ");
      break;

    case PlanNodeType::SEQSCAN:
      child_executor = new executor::SeqScanExecutor(plan, executor_context);
      break;

    case PlanNodeType::INDEXSCAN:
      child_executor = new executor::IndexScanExecutor(plan, executor_context);
      break;

    case PlanNodeType::INSERT:
      child_executor = new executor::InsertExecutor(plan, executor_context);
      break;

    case PlanNodeType::DELETE:
      child_executor = new executor::DeleteExecutor(plan, executor_context);
      break;

    case PlanNodeType::UPDATE:
      child_executor = new executor::UpdateExecutor(plan, executor_context);
      break;

    case PlanNodeType::LIMIT:
      child_executor = new executor::LimitExecutor(plan, executor_context);
      break;

    case PlanNodeType::NESTLOOP:
      child_executor =
          new executor::NestedLoopJoinExecutor(plan, executor_context);
      break;

    case PlanNodeType::MERGEJOIN:
      child_executor = new executor::MergeJoinExecutor(plan, executor_context);
      break;

    case PlanNodeType::HASH:
      child_executor = new executor::HashExecutor(plan, executor_context);
      break;

    case PlanNodeType::HASHJOIN:
      child_executor = new executor::HashJoinExecutor(plan, executor_context);
      break;

    case PlanNodeType::PROJECTION:
      child_executor = new executor::ProjectionExecutor(plan, executor_context);
      break;

    case PlanNodeType::MATERIALIZE:
      child_executor =
          new executor::MaterializationExecutor(plan, executor_context);
      break;

    case PlanNodeType::AGGREGATE_V2:
      child_executor = new executor::AggregateExecutor(plan, executor_context);
      break;

    case PlanNodeType::ORDERBY:
      child_executor = new executor::OrderByExecutor(plan, executor_context);
      break;

    case PlanNodeType::DROP:
      child_executor = new executor::DropExecutor(plan, executor_context);
      break;

    case PlanNodeType::ANALYZE:
      child_executor = new executor::AnalyzeExecutor(plan, executor_context);
      break;

    case PlanNodeType::CREATE:
      child_executor = new executor::CreateExecutor(plan, executor_context);
      break;
    case PlanNodeType::COPY:
      child_executor = new executor::CopyExecutor(plan, executor_context);
      break;
    case PlanNodeType::POPULATE_INDEX:
      child_executor =
          new executor::PopulateIndexExecutor(plan, executor_context);
      break;
    default:
      LOG_ERROR("Unsupported plan node type : %s",
                PlanNodeTypeToString(plan_node_type).c_str());
      break;
  }
  LOG_TRACE("Adding %s Executor", PlanNodeTypeToString(plan_node_type).c_str());

  // Base case
  if (child_executor != nullptr) {
    if (root != nullptr)
      root->AddChild(child_executor);
    else
      root = child_executor;
  }

  // Recurse
  auto &children = plan->GetChildren();
  for (auto &child : children) {
    child_executor = BuildExecutorTree(child_executor, child.get(),
                                       executor_context);
  }

  return root;
}

/**
 * @brief Clean up the executor tree.
 * @param The current executor tree
 * @return none.
 */
void CleanExecutorTree(executor::AbstractExecutor *root) {
  if (root == nullptr) return;

  // Recurse
  auto children = root->GetChildren();
  for (auto child : children) {
    CleanExecutorTree(child);
    delete child;
  }
}

}  // namespace executor
}  // namespace peloton
