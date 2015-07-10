/*-------------------------------------------------------------------------
 *
 * plan_executor.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/plan_executor.cpp
 *
 *-------------------------------------------------------------------------
 */


#include "backend/bridge/plan_executor.h"
#include "backend/bridge/tuple_transformer.h"
#include "backend/common/logger.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/executor/executors.h"
#include "backend/storage/tile_iterator.h"

#include "access/tupdesc.h"
#include "nodes/print.h"
#include "utils/memutils.h"

namespace peloton {
namespace bridge {

executor::AbstractExecutor *BuildExecutorTree(executor::AbstractExecutor *root,
                                              planner::AbstractPlanNode *plan,
                                              concurrency::Transaction *txn,
                                              bool materialized = false);

void CleanExecutorTree(executor::AbstractExecutor *root);

/**
 * @brief Pretty print the plan tree.
 * @param The plan tree
 * @return none.
 */
void PlanExecutor::PrintPlan(const planner::AbstractPlanNode *plan, std::string prefix) {
  if(plan == nullptr)
    return;

  prefix += "  ";

  LOG_INFO("%s->Plan Type :: %d ", prefix.c_str(), plan->GetPlanNodeType());

  auto children = plan->GetChildren();

  for(auto child : children) {
    PrintPlan(child, prefix);
  }

}

/**
 * @brief Build the executor tree.
 * @param The current executor tree
 * @param The plan tree
 * @param Transation context
 * @return The updated executor tree.
 */
executor::AbstractExecutor *BuildExecutorTree(executor::AbstractExecutor *root,
                                              planner::AbstractPlanNode *plan,
                                              concurrency::Transaction *txn,
                                              bool materialized) {
  // Base case
  if(plan == nullptr)
    return root;

  executor::AbstractExecutor *child_executor = nullptr;

  auto plan_node_type = plan->GetPlanNodeType();
  switch(plan_node_type) {
    case PLAN_NODE_TYPE_INVALID:
      LOG_ERROR("Invalid plan node type ");
      break;

    case PLAN_NODE_TYPE_SEQSCAN:
      // Materialized ?
      if(materialized == false) {
        child_executor = new executor::MaterializationExecutor(nullptr);
        materialized = true;
        break;
      }

      // Already materialized.
      child_executor = new executor::SeqScanExecutor(plan, txn);
      materialized = false;
      break;

    case PLAN_NODE_TYPE_INSERT:
      child_executor = new executor::InsertExecutor(plan, txn);
      break;

    case PLAN_NODE_TYPE_DELETE:
      child_executor = new executor::DeleteExecutor(plan, txn);
      break;

    case PLAN_NODE_TYPE_LIMIT:
      child_executor = new executor::LimitExecutor(plan, txn);
      break;

    default:
      LOG_INFO("Unsupported plan node type : %d ", plan_node_type);
      break;
  }

  // Base case
  if(child_executor != nullptr) {
    if(root != nullptr)
      root->AddChild(child_executor);
    else
      root = child_executor;
  }

  // Recurse
  auto children = plan->GetChildren();
  for(auto child : children) {
    child_executor = BuildExecutorTree(child_executor, child, txn);
  }

  // Materialize
  if(materialized == true) {
    BuildExecutorTree(child_executor, plan, txn, true);
  }

  return root;
}

/**
 * @brief Clean up the executor tree.
 * @param The current executor tree
 * @return none.
 */
void CleanExecutorTree(executor::AbstractExecutor *root){
  if(root == nullptr)
    return;

  // Recurse
  auto children = root->GetChildren();
  for(auto child : children) {
    CleanExecutorTree(child);
  }

  // Cleanup self
  delete root;
}


/**
 * @brief Build a executor tree and execute it.
 * @return status of execution.
 */
bool PlanExecutor::ExecutePlan(planner::AbstractPlanNode *plan,
                               TupleDesc tuple_desc,
                               Peloton_Status *pstatus) {

  assert(plan);

  bool status;
  MemoryContext oldContext;
  List *slots = NULL;

  auto& txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  LOG_TRACE("Building the executor tree");

  // Build the executor tree
  executor::AbstractExecutor *executor_tree = BuildExecutorTree(nullptr,
                                                                plan, txn);

  LOG_TRACE("Initializing the executor tree");

  // Initialize the executor tree
  status = executor_tree->Init();

  // Abort and cleanup
  if(status == false) {
    txn_manager.AbortTransaction(txn);
    txn_manager.EndTransaction(txn);
    CleanExecutorTree(executor_tree);
    return false;
  }

  LOG_TRACE("Running the executor tree");

  // Execute the tree until we get result tiles from root node
  for(;;) {
    status = executor_tree->Execute();

    // Abort and cleanup
    if(status == false) {
      txn_manager.AbortTransaction(txn);
      txn_manager.EndTransaction(txn);
      CleanExecutorTree(executor_tree);
      return false;
    }

    // Try to print the first tile's content
    std::unique_ptr<executor::LogicalTile> tile(executor_tree->GetOutput());

    if(tile.get() == nullptr)
      break;

    // Get result base tile and iterate over it
    auto base_tile = tile.get()->GetBaseTile(0);
    assert(base_tile);
    storage::TileIterator tile_itr(base_tile);
    storage::Tuple tuple(base_tile->GetSchema());

    // Switch to TopSharedMemoryContext to construct list and slots
    oldContext = MemoryContextSwitchTo(TopSharedMemoryContext);

    // Go over tile and get result slots
    while (tile_itr.Next(tuple)) {
      auto slot = TupleTransformer::GetPostgresTuple(&tuple, tuple_desc);
      slots = lappend(slots, slot);
    }

    // Go back to previous context
    MemoryContextSwitchTo(oldContext);

  }

  pstatus->m_result_slots = slots;

  // Commit and cleanup
  txn_manager.CommitTransaction(txn);
  txn_manager.EndTransaction(txn);
  CleanExecutorTree(executor_tree);

  return true;
}


}  // namespace bridge
}  // namespace peloton


