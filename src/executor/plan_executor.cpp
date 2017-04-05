//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_executor.cpp
//
// Identification: src/executor/plan_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "executor/plan_executor.h"

#include <vector>

#include "common/logger.h"
#include "executor/executor_context.h"
#include "executor/executors.h"
#include "optimizer/util.h"
#include "storage/tuple_iterator.h"

namespace peloton {
namespace executor {

/*
 * Added for network invoking efficiently
 */
executor::ExecutorContext *BuildExecutorContext(
    const std::vector<type::Value> &params, concurrency::Transaction *txn);

executor::AbstractExecutor *BuildExecutorTree(
    executor::AbstractExecutor *root, const planner::AbstractPlan *plan,
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
ExecuteResult PlanExecutor::ExecutePlan(
    const planner::AbstractPlan *plan, concurrency::Transaction *txn,
    const std::vector<type::Value> &params, std::vector<StatementResult> &result,
    const std::vector<int> &result_format) {
  ExecuteResult p_status;
  if (plan == nullptr) return p_status;

  LOG_TRACE("PlanExecutor Start ");

  bool status;

  PL_ASSERT(txn);

  LOG_TRACE("Txn ID = %lu ", txn->GetTransactionId());
  LOG_TRACE("Building the executor tree");

  // Use const std::vector<type::Value> &params to make it more elegant for
  // network
  std::unique_ptr<executor::ExecutorContext> executor_context(
      BuildExecutorContext(params, txn));

  // Build the executor tree
  std::unique_ptr<executor::AbstractExecutor> executor_tree(
      BuildExecutorTree(nullptr, plan, executor_context.get()));

  LOG_TRACE("Initializing the executor tree");

  // Initialize the executor tree
  status = executor_tree->Init();

  if (status == true) {
    LOG_TRACE("Running the executor tree");
    result.clear();

    // Execute the tree until we get result tiles from root node
    while (status == true) {
      status = executor_tree->Execute();

      std::unique_ptr<executor::LogicalTile> logical_tile(
          executor_tree->GetOutput());
      // Some executors don't return logical tiles (e.g., Update).
      if (logical_tile.get() != nullptr) {
        LOG_TRACE("Final Answer: %s",
                  logical_tile->GetInfo().c_str());  // Printing the answers
        std::vector<std::vector<std::string>> answer_tuples;
        answer_tuples = std::move(
            logical_tile->GetAllValuesAsStrings(result_format, false));

        // Construct the returned results
        for (auto &tuple : answer_tuples) {
          unsigned int col_index = 0;
          for (unsigned int i = 0; i < logical_tile->GetColumnCount(); i++) {
            auto res = StatementResult();
            PlanExecutor::copyFromTo(tuple[col_index++], res.second);
            if (tuple[col_index - 1].c_str() != nullptr) {
              LOG_TRACE("column content: %s", tuple[col_index - 1].c_str());
            }
            result.push_back(std::move(res));
          }
        }
      }
    }

    // Set the result
    p_status.m_processed = executor_context->num_processed;
    // success so far
    p_status.m_result = ResultType::SUCCESS;
  } else {
    p_status.m_result = ResultType::FAILURE;
  }

  p_status.m_result_slots = nullptr;

  // clean up executor tree
  CleanExecutorTree(executor_tree.get());

  return p_status;
}

/**
 * @brief Build a executor tree and execute it.
 * Use std::vector<type::Value> as params to make it more elegant for
 * networking
 * Before ExecutePlan, a node first receives value list, so we should pass
 * value list directly rather than passing Postgres's ParamListInfo
 * @return number of executed tuples and logical_tile_list
 */
int PlanExecutor::ExecutePlan(
    const planner::AbstractPlan *plan, const std::vector<type::Value> &params,
    std::vector<std::unique_ptr<executor::LogicalTile>> &logical_tile_list) {
  if (plan == nullptr) return -1;

  LOG_TRACE("PlanExecutor Start ");

  bool status;
  bool init_failure = false;
  bool single_statement_txn = false;

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // auto txn = peloton::concurrency::current_txn;

  // This happens for single statement queries in PG
  // if (txn == nullptr) {
  single_statement_txn = true;
  auto txn = txn_manager.BeginTransaction();
  // }
  PL_ASSERT(txn);

  LOG_TRACE("Txn ID = %lu ", txn->GetTransactionId());
  LOG_TRACE("Building the executor tree");

  // Use const std::vector<type::Value> &params to make it more elegant for
  // network
  std::unique_ptr<executor::ExecutorContext> executor_context(
      BuildExecutorContext(params, txn));

  // Build the executor tree
  std::unique_ptr<executor::AbstractExecutor> executor_tree(
      BuildExecutorTree(nullptr, plan, executor_context.get()));

  LOG_TRACE("Initializing the executor tree");

  // Initialize the executor tree
  status = executor_tree->Init();

  // Abort and cleanup
  if (status == false) {
    init_failure = true;
    txn->SetResult(ResultType::FAILURE);
    goto cleanup;
  }

  LOG_TRACE("Running the executor tree");

  // Execute the tree until we get result tiles from root node
  for (;;) {
    status = executor_tree->Execute();

    // Stop
    if (status == false) {
      break;
    }

    std::unique_ptr<executor::LogicalTile> logical_tile(
        executor_tree->GetOutput());

    // Some executors don't return logical tiles (e.g., Update).
    if (logical_tile.get() == nullptr) {
      continue;
    }

    logical_tile_list.push_back(std::move(logical_tile));
  }

// final cleanup
cleanup:

  LOG_TRACE("About to commit: single stmt: %d, init_failure: %d, status: %s",
            single_statement_txn, init_failure,
            ResultTypeToString(txn->GetResult()).c_str());

  // clean up executor tree
  CleanExecutorTree(executor_tree.get());

  // should we commit or abort ?
  if (single_statement_txn == true || init_failure == true) {
    auto status = txn->GetResult();
    switch (status) {
      case ResultType::SUCCESS:
        // Commit
        return executor_context->num_processed;

        break;

      case ResultType::FAILURE:
      default:
        // Abort
        return -1;
    }
  }
  return executor_context->num_processed;
}

/**
 * @brief Build Executor Context
 */
executor::ExecutorContext *BuildExecutorContext(
    const std::vector<type::Value> &params, concurrency::Transaction *txn) {
  return new executor::ExecutorContext(txn, params);
}

/**
 * @brief Build the executor tree.
 * @param The current executor tree
 * @param The plan tree
 * @param Transation context
 * @return The updated executor tree.
 */
executor::AbstractExecutor *BuildExecutorTree(
    executor::AbstractExecutor *root, const planner::AbstractPlan *plan,
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
      LOG_TRACE("Adding Sequential Scan Executer");
      child_executor = new executor::SeqScanExecutor(plan, executor_context);
      break;

    case PlanNodeType::INDEXSCAN:
      LOG_TRACE("Adding Index Scan Executer");
      child_executor = new executor::IndexScanExecutor(plan, executor_context);
      break;

    case PlanNodeType::INSERT:
      LOG_TRACE("Adding Insert Executer");
      child_executor = new executor::InsertExecutor(plan, executor_context);
      break;

    case PlanNodeType::DELETE:
      LOG_TRACE("Adding Delete Executer");
      child_executor = new executor::DeleteExecutor(plan, executor_context);
      break;

    case PlanNodeType::UPDATE:
      LOG_TRACE("Adding Update Executer");
      child_executor = new executor::UpdateExecutor(plan, executor_context);
      break;

    case PlanNodeType::LIMIT:
      LOG_TRACE("Adding Limit Executer");
      child_executor = new executor::LimitExecutor(plan, executor_context);
      break;

    case PlanNodeType::NESTLOOP:
      LOG_TRACE("Adding Nested Loop Joing Executer");
      child_executor =
          new executor::NestedLoopJoinExecutor(plan, executor_context);
      break;

    case PlanNodeType::MERGEJOIN:
      LOG_TRACE("Adding Merge Join Executer");
      child_executor = new executor::MergeJoinExecutor(plan, executor_context);
      break;

    case PlanNodeType::HASH:
      LOG_TRACE("Adding Hash Executer");
      child_executor = new executor::HashExecutor(plan, executor_context);
      break;

    case PlanNodeType::HASHJOIN:
      LOG_TRACE("Adding Hash Join Executer");
      child_executor = new executor::HashJoinExecutor(plan, executor_context);
      break;

    case PlanNodeType::PROJECTION:
      LOG_TRACE("Adding Projection Executer");
      child_executor = new executor::ProjectionExecutor(plan, executor_context);
      break;

    case PlanNodeType::MATERIALIZE:
      LOG_TRACE("Adding Materialization Executer");
      child_executor =
          new executor::MaterializationExecutor(plan, executor_context);
      break;

    case PlanNodeType::AGGREGATE_V2:
      LOG_TRACE("Adding Aggregate Executer");
      child_executor = new executor::AggregateExecutor(plan, executor_context);
      break;

    case PlanNodeType::ORDERBY:
      LOG_TRACE("Adding Order By Executer");
      child_executor = new executor::OrderByExecutor(plan, executor_context);
      break;

    case PlanNodeType::DROP:
      LOG_TRACE("Adding Drop Executer");
      child_executor = new executor::DropExecutor(plan, executor_context);
      break;

    case PlanNodeType::CREATE:
      LOG_TRACE("Adding Create Executer");
      child_executor = new executor::CreateExecutor(plan, executor_context);
      break;
    case PlanNodeType::COPY:
      LOG_TRACE("Adding Copy Executer");
      child_executor = new executor::CopyExecutor(plan, executor_context);
      break;
    case PlanNodeType::POPULATE_INDEX:
      LOG_TRACE("Adding PopulateIndex Executor");
      child_executor = new executor::PopulateIndexExecutor(plan, executor_context);
      break;
    default:
      LOG_ERROR("Unsupported plan node type : %s",
                PlanNodeTypeToString(plan_node_type).c_str());
      break;
  }

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
    child_executor =
        BuildExecutorTree(child_executor, child.get(), executor_context);
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
