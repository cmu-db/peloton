#include "executor/alter_executor.h"

#include "catalog/catalog.h"
#include "common/logger.h"
#include "executor/executor_context.h"

namespace peloton {
namespace executor {

// Constructor for alter table executor
AlterExecutor::AlterExecutor(const planner::AbstractPlan *node,
                             ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {
  context_ = executor_context;
}

// Initialize executor
// Nothing to initialize for now
bool AlterExecutor::DInit() {
  LOG_TRACE("Initializing Alter Executer...");

  LOG_TRACE("Alter Executor initialized!");
  return true;
}

bool AlterExecutor::DExecute() {
  LOG_TRACE("Executing Drop...");
  bool result = false;
  const planner::RenamePlan &node = GetPlanNode<planner::RenamePlan>();
  auto current_txn = context_->GetTransaction();
  PlanNodeType plan_node_type = node.GetPlanNodeType();
  if (plan_node_type == PlanNodeType::RENAME) {
    result = RenameColumn(node, current_txn);
  } else if (plan_node_type == PlanNodeType::ALTER) {
    LOG_TRACE("Will perform alter table operations");
  } else {
    throw NotImplementedException(
        StringUtil::Format("Plan node type not supported, %s",
                           PlanNodeTypeToString(plan_node_type).c_str()));
  }

  return result;
}

bool AlterExecutor::RenameColumn(
    const peloton::planner::RenamePlan &node,
    peloton::concurrency::TransactionContext *txn) {
  auto database_name = node.GetDatabaseName();
  auto table_name = node.GetTableName();
  auto new_column_name = node.GetNewName();
  auto old_column_name = node.GetOldName();

  std::vector<std::string> old_names = {old_column_name};
  std::vector<std::string> names = {new_column_name};
  ResultType result = catalog::Catalog::GetInstance()->ChangeColumnName(
      database_name, table_name, old_names, names, txn);
  txn->SetResult(result);

  if (txn->GetResult() == ResultType::SUCCESS) {
    LOG_TRACE("Rename column succeeded!");

    // Add on succeed logic if necessary
  } else {
    LOG_TRACE("Result is: %s", ResultTypeToString(txn->GetResult()).c_str());
  }
  return false;
}

}  // executor
}  // peloton
