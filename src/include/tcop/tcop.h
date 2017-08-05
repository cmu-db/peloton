//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// traffic_cop.h
//
// Identification: src/include/traffic_cop/traffic_cop.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
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

namespace peloton {

namespace tcop {
//===--------------------------------------------------------------------===//
// TRAFFIC COP
//===--------------------------------------------------------------------===//

class TrafficCop {
  TrafficCop(TrafficCop const &) = delete;

 public:
  TrafficCop();
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
      std::vector<StatementResult> &result, int &rows_change,
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

 private:

  // The optimizer used for this connection
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer_;

  // flag of single statement txn
  bool single_statement_txn_;

  // flag of psql protocol
  ResultType result_;
//  executor::ExecuteResult;
  executor::ExecuteResult p_status_;

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

  void ExecuteStatementPlanGetResult(executor::ExecuteResult &p_status,
                                                                    concurrency::Transaction *txn);

//  const std::shared_ptr<Statement> statement_;
//  const std::vector<type::Value> params_;
//  UNUSED_ATTRIBUTE const bool unnamed;
//  std::shared_ptr<stats::QueryMetric::QueryParams> param_stats_;
//  const std::vector<int> &result_format, std::vector<StatementResult> result;
//  int &rows_changed, UNUSED_ATTRIBUTE std::string error_message;
//  const size_t thread_id UNUSED_ATTRIBUTE;
};

}  // namespace traffic_cop
}  // namespace peloton
