//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// t=cop.h
//
// Identification: src/include/traffic_cop/tcop.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include "network/connection_handle.h"
#include "parser/postgresparser.h"
#include "parser/sql_statement.h"
#include "common/statement_cache.h"
#include "client_transaction_handle.h"
namespace peloton {
namespace tcop {

// pair of txn ptr and the result so-far for that txn
// use a stack to support nested-txns
using TcopTxnState = std::pair<concurrency::TransactionContext *, ResultType>;

// TODO(Tianyu): Probably need a better name
// TODO(Tianyu): We can probably get rid of a bunch of fields from here
struct ClientProcessState {
  size_t thread_id_;
  bool is_queuing_;
  std::string error_message_, db_name_ = DEFAULT_DB_NAME;
  std::vector<type::Value> param_values_;
  // This save currnet statement in the traffic cop
  std::shared_ptr<Statement> statement_;
  // The optimizer used for this connection
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer_;
  // flag of single statement txn
  std::vector<ResultValue> result_;
  executor::ExecutionResult p_status_;
  StatementCache statement_cache_;

  // Transaction Handling Wrapper
  ClientTxnHandle txn_handle_;

  // The current callback to be invoked after execution completes.
  void (*task_callback_)(void *);
  void *task_callback_arg_;
};

// Execute a statement
ResultType ExecuteStatement(
    ClientProcessState &state,
    const std::vector<int> &result_format, std::vector<ResultValue> &result);

// Helper to handle txn-specifics for the plan-tree of a statement.
void ExecuteHelper(
    ClientProcessState &state,
    std::vector<ResultValue> &result,
    const std::vector<int> &result_format,
    concurrency::TransactionContext *txn);

// Prepare a statement
bool PrepareStatement(
    ClientProcessState &state, const std::string &query_string,
    const std::string &statement_name = "unnamed");

bool BindParamsForCachePlan(
    ClientProcessState &state,
    const std::vector<std::unique_ptr<expression::AbstractExpression>> &,
    size_t thread_id = 0);

std::vector<FieldInfo> GenerateTupleDescriptor(
    parser::SQLStatement *select_stmt);

FieldInfo GetColumnFieldForValueType(std::string column_name,
                                     type::TypeId column_type);

void ExecuteStatementPlanGetResult();

ResultType ExecuteStatementGetResult();

void ProcessInvalidStatement(ClientProcessState &state);

// Get all data tables from a TableRef.
// For multi-way join
// still a HACK
void GetTableColumns(parser::TableRef *from_table,
                     std::vector<catalog::Column> &target_tables);

} // namespace tcop
} // namespace peloton