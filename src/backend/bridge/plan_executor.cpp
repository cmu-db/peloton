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
#include "backend/common/logger.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/executor/executors.h"

namespace peloton {
namespace bridge {

executor::AbstractExecutor *BuildExecutorTree(executor::AbstractExecutor *root,
                                              planner::AbstractPlanNode *plan,
                                              concurrency::Transaction *txn);

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

  LOG_INFO("%s->Plan Type :: %d \n", prefix.c_str(), plan->GetPlanNodeType());

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
                                              concurrency::Transaction *txn) {
  // Base case
  if(plan == nullptr)
    return root;

  executor::AbstractExecutor *child_executor = nullptr;

  auto plan_node_type = plan->GetPlanNodeType();
  switch(plan_node_type) {
    case PLAN_NODE_TYPE_INVALID:
      LOG_ERROR("Invalid plan node type \n");
      break;

    case PLAN_NODE_TYPE_SEQSCAN:
      child_executor = new executor::SeqScanExecutor(plan, txn);
      break;

    case PLAN_NODE_TYPE_INSERT:
      child_executor = new executor::InsertExecutor(plan, txn);
      break;

    case PLAN_NODE_TYPE_DELETE:
      child_executor = new executor::DeleteExecutor(plan, txn);
      break;

    default:
      LOG_INFO("Unsupported plan node type : %d \n", plan_node_type);
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
bool PlanExecutor::ExecutePlan(planner::AbstractPlanNode *plan) {

  assert(plan);

  auto& txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  LOG_INFO("Building the executor tree\n");

  // Build the executor tree
  executor::AbstractExecutor *executor_tree = BuildExecutorTree(nullptr, plan, txn);

  LOG_INFO("Initializing the executor tree\n");
  // Init the executor tree
  bool status_init = executor_tree->Init();

  assert(status_init);

  LOG_INFO("Running the executor tree\n");

  // Execute the tree
  bool status = executor_tree->Execute();

  if(status == true) {
    // Try to print the first tile's content
    std::unique_ptr<executor::LogicalTile> tile(executor_tree->GetOutput());
    if(tile.get()){
      std::cout << *tile.get() << std::endl;
      for(auto tuple_id : *tile){
        for(oid_t col_id = 0; col_id < tile->GetColumnCount(); col_id++)
          std::cout << tile->GetValue(tuple_id, col_id) << ", ";
        std::cout << std::endl;
      }
    }
    txn_manager.CommitTransaction(txn);
  }
  else
    txn_manager.AbortTransaction(txn);

  txn_manager.EndTransaction(txn);

  LOG_INFO("Cleaning up the executor tree\n");

  // Clean up the executor tree
  CleanExecutorTree(executor_tree);

  return status;
}


}  // namespace bridge
}  // namespace peloton


