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

#include "executor/plan_executor.h"
#include "optimizer/abstract_optimizer.h"
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
  bool is_queuing_ = false;
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
};

/**
 * Build and optimized a statement from raw query string and store it in state
 * @param state Client state context
 * @param query_string raw query in string format
 * @param statement_name Name of the statement stored in the statement cache
 * @return true if the statement is created
 */
bool PrepareStatement(
    ClientProcessState &state, const std::string &query_string,
    const std::string &statement_name = "unnamed");

/**
 * Execute the statemnet attached in state
 * @param state Client state context
 * @param result_format expected result's format specified by the protocol
 * @param result the vector to store the result being returned
 * @param callback callback function to be invoke after the execution is finished. Only useful if the return type is false
 * @return true if the execution is finished
 */
bool ExecuteStatement(
    ClientProcessState &state,
    const std::vector<int> &result_format,
    std::vector<ResultValue> &result,
    callback_func callback);

/**
 * Helper function to submit the executable plan to worker pool
 * @param state Client state context
 * @param result_format expected result's format specified by the protocol
 * @param result the vector to store the result being returned
 * @param txn Transaction context
 * @param callback callback function to be invoke after the execution is finished. Only useful if the return type is false
 */
void ExecuteHelper(
    ClientProcessState &state,
    std::vector<ResultValue> &result,
    const std::vector<int> &result_format,
    concurrency::TransactionContext *txn,
    callback_func callback);

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