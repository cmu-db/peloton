//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// traffic_cop.h
//
// Identification: src/include/traffic_cop/traffic_cop.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <stack>
#include <vector>

#include "common/portal.h"
#include "common/statement.h"
#include "concurrency/transaction_context.h"
#include "executor/plan_executor.h"
#include "optimizer/abstract_optimizer.h"
#include "parser/sql_statement.h"
#include "storage/data_table.h"
#include "logging/wal_log_manager.h"
#include "type/type.h"
#include "common/internal_types.h"
#include "event.h"

namespace peloton {
namespace tcop {

//===--------------------------------------------------------------------===//
// TRAFFIC COP
// Helpers for executing statements.
//
// Usage in unit tests:
//   auto &traffic_cop = tcop::TrafficCop::GetInstance();
//   traffic_cop.SetTaskCallback(<callback>, <arg>);
//   txn = txn_manager.BeginTransaction();
//   traffic_cop.SetTcopTxnState(txn);
//   std::shared_ptr<AbstractPlan> plan = <set up a plan>;
//   traffic_cop.ExecuteHelper(plan, <params>, <result>, <result_format>);
//   <wait>
//   traffic_cop.CommitQueryHelper();
//===--------------------------------------------------------------------===//

class TrafficCop {
 public:
  TrafficCop();
  TrafficCop(void (*task_callback)(void *), void *task_callback_arg);
  ~TrafficCop();
  DISALLOW_COPY_AND_MOVE(TrafficCop);

  // Static singleton used by unit tests.
  static TrafficCop &GetInstance();

  // Reset this object.
  void Reset();

  // Execute a statement from a prepared and bound statement.
  ResultType ExecuteStatement(
      const std::shared_ptr<Statement> &statement,
      const std::vector<type::Value> &params, bool unnamed,
      std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,
      const std::vector<int> &result_format, std::vector<ResultValue> &result,
      std::string &error_message, logging::WalLogManager *log_manager,
      const size_t thread_id = 0);
  ResultType ExecuteStatement(
      const std::shared_ptr<Statement> &statement,
      const std::vector<type::Value> &params, const bool unnamed,
      std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,
      const std::vector<int> &result_format,
      std::vector<StatementResult> &result, int &rows_change,
      std::string &error_message, const size_t thread_id = 0) {
    return ExecuteStatement(statement, params, unnamed, param_stats,
                            result_format, result, rows_change, error_message,
                            nullptr, thread_id);
  }

  // Helper to handle txn-specifics for the plan-tree of a statement.
  executor::ExecutionResult ExecuteHelper(
      std::shared_ptr<planner::AbstractPlan> plan,
      const std::vector<type::Value> &params, std::vector<ResultValue> &result,
      const std::vector<int> &result_format, size_t thread_id = 0);

  // Prepare and bind a query from a query string
  std::shared_ptr<Statement> PrepareStatement(const std::string &statement_name,
                                              const std::string &query_string,
                                              std::string &error_message,
                                              size_t thread_id = 0);

  std::vector<FieldInfo> GenerateTupleDescriptor(
      parser::SQLStatement *select_stmt);

  FieldInfo GetColumnFieldForValueType(std::string column_name,
                                       type::TypeId column_type);

  void SetTcopTxnState(concurrency::TransactionContext *txn) {
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  ResultType CommitQueryHelper(logging::WalLogManager *log_manager);

  void ExecuteStatementPlanGetResult(logging::WalLogManager *log_manager);

  ResultType ExecuteStatementGetResult();
  ResultType CommitQueryHelper() { return CommitQueryHelper(nullptr); }

  void ExecuteStatementPlanGetResult() {
    ExecuteStatementPlanGetResult(nullptr);
  }
    ExecuteStatementPlanGetResult(nullptr);
  }

  void SetTaskCallback(void (*task_callback)(void *), void *task_callback_arg) {
    task_callback_ = task_callback;
    task_callback_arg_ = task_callback_arg;
  }

  void setRowsAffected(int rows_affected) { rows_affected_ = rows_affected; }

  int getRowsAffected() { return rows_affected_; }

  void SetStatement(std::shared_ptr<Statement> statement) {
    statement_ = std::move(statement);
  }

  std::shared_ptr<Statement> GetStatement() { return statement_; }

  void SetResult(std::vector<ResultValue> result) {
    result_ = std::move(result);
  }

  std::vector<ResultValue> &GetResult() { return result_; }

  void SetParamVal(std::vector<type::Value> param_values) {
    param_values_ = std::move(param_values);
  }

  std::vector<type::Value> &GetParamVal() { return param_values_; }

  void SetErrorMessage(std::string error_message) {
    error_message_ = std::move(error_message);
  }

  std::string &GetErrorMessage() { return error_message_; }

  void SetQueuing(bool is_queuing) { is_queuing_ = is_queuing; }

  bool GetQueuing() { return is_queuing_; }

  executor::ExecutionResult p_status_;

  void SetDefaultDatabaseName(std::string default_database_name) {
    default_database_name_ = std::move(default_database_name);
  }

  // TODO: this member variable should be in statement_ after parser part
  // finished
  std::string query_;

 private:
  bool is_queuing_;

  std::string error_message_;

  std::vector<type::Value> param_values_;

  std::vector<ResultValue> results_;

  // This save currnet statement in the traffic cop
  std::shared_ptr<Statement> statement_;

  // Default database name
  std::string default_database_name_ = DEFAULT_DB_NAME;

  int rows_affected_;

  // The optimizer used for this connection
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer_;

  // flag of single statement txn
  bool is_single_statement_txn_;

  std::vector<ResultValue> result_;

  // The current callback to be invoked after execution completes.
  void (*task_callback_)(void *);
  void *task_callback_arg_;

  // pair of txn ptr and the result so-far for that txn
  // use a stack to support nested-txns
  using TcopTxnState = std::pair<concurrency::TransactionContext *, ResultType>;
  std::stack<TcopTxnState> tcop_txn_state_;

  static TcopTxnState &GetDefaultTxnState();

  TcopTxnState &GetCurrentTxnState();

  ResultType BeginQueryHelper(size_t thread_id);

  ResultType AbortQueryHelper();

  // Get all data tables from a TableRef.
  // For multi-way join
  // still a HACK
  void GetDataTables(parser::TableRef *from_table,
                     std::vector<storage::DataTable *> &target_tables);
};

}  // namespace tcop
}  // namespace peloton
