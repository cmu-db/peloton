//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// analyze_executor.cpp
//
// Identification: src/executor/analyze_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "executor/analyze_executor.h"
#include "executor/executor_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "common/logger.h"
#include "catalog/catalog.h"
#include "optimizer/stats/stats_storage.h"

namespace peloton {
namespace executor {

AnalyzeExecutor::AnalyzeExecutor(const planner::AbstractPlan *node,
                                 ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context),
      executor_context_(executor_context) {}

bool AnalyzeExecutor::DInit() {
  LOG_TRACE("Initializing analyze executor...");
  LOG_TRACE("Analyze executor initialized!");
  return true;
}

bool AnalyzeExecutor::DExecute() {
  LOG_TRACE("Executing Analyze...");

  const planner::AnalyzePlan &node = GetPlanNode<planner::AnalyzePlan>();

  storage::DataTable* target_table = node.GetTable();
  UNUSED_ATTRIBUTE std::vector<char*> target_columns = node.GetColumnNames();

  // LOG_TRACE("Analyzing column size %lu", target_columns.size());

  auto current_txn = executor_context_->GetTransaction();

  auto stats = optimizer::StatsStorage::GetInstance();

  if (target_table != nullptr) {
    LOG_TRACE("Analyzing table %s", node.GetTableName().c_str());
    ResultType result = stats->AnalyzeStatsForTable(target_table, current_txn);
    current_txn->SetResult(result);
    if (result == peloton::ResultType::SUCCESS) {
      LOG_TRACE("Successfully analyzed table %s", node.GetTableName().c_str());
    } else {
      LOG_TRACE("Failed to analyze table %s", node.GetTableName().c_str());
    }
  } else {
    // other operations unsupported for now
    current_txn->SetResult(peloton::ResultType::SUCCESS);
  }

  LOG_TRACE("Analyzing finished!");
  return false;
}

}  // namespace executor
}  // namespace peloton
