//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tcop.h
//
// Identification: src/include/tcop/tcop.h
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
  Result ExecuteStatement(const std::string &query,
                          std::vector<ResultType> &result,
                          std::vector<FieldInfoType> &tuple_descriptor,
                          int &rows_changed, std::string &error_message);

  // ExecPrepStmt - Execute a statement from a prepared and bound statement
  Result ExecuteStatement(
      const std::shared_ptr<Statement> &statement,
      const std::vector<type::Value> &params, const bool unnamed,
      std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,
      const std::vector<int> &result_format, std::vector<ResultType> &result,
      int &rows_change, std::string &error_message);

  // ExecutePrepStmt - Helper to handle txn-specifics for the plan-tree of a
  // statement
  bridge::peloton_status ExecuteStatementPlan(
      const planner::AbstractPlan *plan, const std::vector<type::Value> &params,
      std::vector<ResultType> &result, const std::vector<int> &result_format);

  // InitBindPrepStmt - Prepare and bind a query from a query string
  std::shared_ptr<Statement> PrepareStatement(const std::string &statement_name,
                                              const std::string &query_string,
                                              std::string &error_message);

  std::vector<FieldInfoType> GenerateTupleDescriptor(
      parser::SQLStatement *select_stmt);

  FieldInfoType GetColumnFieldForValueType(std::string column_name,
                                           type::Type::TypeId column_type);

  FieldInfoType GetColumnFieldForAggregates(std::string name,
                                            ExpressionType expr_type);

  int BindParameters(std::vector<std::pair<int, std::string>> &parameters,
                     Statement **stmt, std::string &error_message);

 private:
  // The optimizer used for this connection
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer_;

  // pair of txn ptr and the result so-far for that txn
  // use a stack to support nested-txns
  typedef std::pair<concurrency::Transaction *, Result> TcopTxnState;
  std::stack<TcopTxnState> tcop_txn_state_;

 private:
  static TcopTxnState &GetDefaultTxnState();

  TcopTxnState &GetCurrentTxnState();

  Result BeginQueryHelper();

  Result CommitQueryHelper();

  Result AbortQueryHelper();
};

}  // End tcop namespace
}  // End peloton namespace
