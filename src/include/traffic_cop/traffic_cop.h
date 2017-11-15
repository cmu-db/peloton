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

#include <stdio.h>
#include <stdlib.h>
#include <mutex>
#include <stack>
#include <vector>

#include "common/portal.h"
#include "common/statement.h"
#include "concurrency/transaction.h"
#include "executor/plan_executor.h"
#include "optimizer/abstract_optimizer.h"
#include "parser/sql_statement.h"
#include "storage/data_table.h"
#include "type/type.h"
#include "type/types.h"
#include "event.h"

namespace peloton {
//void ExecutePlanWrapper(void *arg_ptr);
namespace tcop {

//===--------------------------------------------------------------------===//
// TRAFFIC COP
//===--------------------------------------------------------------------===//

class TrafficCop {
  TrafficCop(TrafficCop const &) = delete;

 public:
  TrafficCop();
  TrafficCop(void(* task_callback)(void *), void *task_callback_arg);

  ~TrafficCop();

  // static singleton method used by tests
  static TrafficCop &GetInstance();

  // reset this object
  void Reset();

  // PortalExec - Execute query string
//  ResultType ExecuteStatement(const std::string &query,
//                              std::vector<StatementResult> &result,
//                              std::vector<FieldInfo> &tuple_descriptor,
//                              int &rows_changed, std::string &error_message,
//                              const size_t thread_id = 0);

  // ExecPrepStmt - Execute a statement from a prepared and bound statement
  ResultType ExecuteStatement(
      const std::shared_ptr<Statement> &statement,
      const std::vector<type::Value> &params, const bool unnamed,
      std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,
      const std::vector<int> &result_format,
      std::vector<StatementResult> &result,
      std::string &error_message, const size_t thread_id = 0);

  // ExecutePrepStmt - Helper to handle txn-specifics for the plan-tree of a
  // statement
  executor::ExecuteResult ExecuteStatementPlan(
      std::shared_ptr<planner::AbstractPlan> plan,
      const std::vector<type::Value> &params,
      std::vector<StatementResult> &result,
      const std::vector<int> &result_format, const size_t thread_id = 0);

  // InitBindPrepStmt - Prepare and bind a query from a query string
  std::shared_ptr<Statement> PrepareStatement(const std::string &statement_name,
                                              const std::string &query_string,
                                              std::string &error_message,
                                              const size_t thread_id = 0);

  std::vector<FieldInfo> GenerateTupleDescriptor(
      parser::SQLStatement *select_stmt);

  FieldInfo GetColumnFieldForValueType(std::string column_name,
                                       type::TypeId column_type);

  FieldInfo GetColumnFieldForAggregates(std::string name,
                                        ExpressionType expr_type);

  int BindParameters(std::vector<std::pair<int, std::string>> &parameters,
                     Statement **stmt, std::string &error_message);

  void SetTcopTxnState(concurrency::Transaction * txn) {
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  ResultType CommitQueryHelper();

  void ExecuteStatementPlanGetResult();

  ResultType ExecuteStatementGetResult();

  static void ExecutePlanWrapper(void *arg_ptr);

  void SetTaskCallback(void(* task_callback)(void*), void *task_callback_arg) {
    task_callback_ = task_callback;
    task_callback_arg_ = task_callback_arg;
  }

  void setRowsAffected(int rows_affected) {
    rows_affected_ = rows_affected;
  }

  int getRowsAffected() {
    return rows_affected_;
  }

  void SetStatement(std::shared_ptr<Statement> statement) {
    statement_ = statement;
  }

  std::shared_ptr<Statement> GetStatement() {
    return statement_;
  }

  void SetResult(std::vector<StatementResult> result) {
    result_ = result;
  }

  std::vector<StatementResult>& GetResult() {
    return result_;
  }

  void SetParamVal(std::vector<type::Value> param_values) {
    param_values_ = param_values;
  }

  std::vector<type::Value>& GetParamVal() {
    return param_values_;
  }

  void SetErrorMessage(std::string error_message) {
    error_message_ = error_message;
  }

  std::string& GetErrorMessage() {
    return error_message_;
  }

  void SetQueuing(bool is_queuing) {
    is_queuing_ = is_queuing;
  }

  bool GetQueuing() {
    return is_queuing_;
  }

  executor::ExecuteResult p_status_;
  
  inline void SetDefaultDatabaseName(std::string default_database_name) {
    default_database_name_ = default_database_name;
  }

  //TODO: I leave this variable here to remind that after parser part finished,
  //TODO: don't forget move query to statement_ according to the way of query_type_
  // QueryType query_type_;

  //TODO: this member variable should be in statement_ after parser part finished
  std::string query_;

//  struct event* event_;
 private:

  bool is_queuing_;

  std::string error_message_;

  //this should stay in traffic_cop, because ExecutePlanArg will use it
  std::vector<type::Value> param_values_;

  //this should stay in traffic_cop, because ExecutePlanArg will use it
  std::vector<StatementResult> results_;

  // This save currnet statement in the traffic cop
  std::shared_ptr<Statement> statement_;

  // Default database name
  std::string default_database_name_ = DEFAULT_DB_NAME;
  
  int rows_affected_;

  // The optimizer used for this connection
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer_;

  // flag of single statement txn
  bool single_statement_txn_;

  // flag of psql protocol
  // executePlan arguments

  std::vector<StatementResult> result_;
  void(* task_callback_)(void *);
  void * task_callback_arg_;
//  IOTrigger io_trigger_;

  // pair of txn ptr and the result so-far for that txn
  // use a stack to support nested-txns
  typedef std::pair<concurrency::Transaction *, ResultType> TcopTxnState;

  std::stack<TcopTxnState> tcop_txn_state_;

  static TcopTxnState &GetDefaultTxnState();

  TcopTxnState &GetCurrentTxnState();

  ResultType BeginQueryHelper(const size_t thread_id);

  ResultType AbortQueryHelper();

  // Get all data tables from a TableRef.
  // For multi-way join
  // still a HACK
  void GetDataTables(parser::TableRef *from_table,
                     std::vector<storage::DataTable *> &target_tables);

//  const std::shared_ptr<Statement> statement_;
//  const std::vector<type::Value> params_;
//  UNUSED_ATTRIBUTE const bool unnamed;
//  std::shared_ptr<stats::QueryMetric::QueryParams> param_stats_;
//  const std::vector<int> &result_format, std::vector<StatementResult> result;
//  int &rows_changed, UNUSED_ATTRIBUTE std::string error_message;
//  const size_t thread_id UNUSED_ATTRIBUTE;
};

//===--------------------------------------------------------------------===//
// TrafficCop: Wrapper struct ExecutePlan argument
//===--------------------------------------------------------------------===//
struct ExecutePlanArg {
  inline ExecutePlanArg(const std::shared_ptr<planner::AbstractPlan> plan,
                        concurrency::Transaction *txn,
                        const std::vector<type::Value> &params,
                        std::vector<StatementResult> &result,
                        const std::vector<int> &result_format,
                        executor::ExecuteResult &p_status) :
      plan_(plan),
      txn_(txn),
      params_(params),
      result_(result),
      result_format_(result_format),
      p_status_(p_status) {}
//      event_(event) {}
//      io_trigger_(io_trigger) { }


  std::shared_ptr<planner::AbstractPlan> plan_;
  concurrency::Transaction *txn_;
  const std::vector<type::Value> &params_;
  std::vector<StatementResult> &result_;
  const std::vector<int> &result_format_;
  executor::ExecuteResult &p_status_;
//  struct event* event_;
//  IOTrigger *io_trigger_;
};

}  // namespace tcop
}  // namespace peloton
