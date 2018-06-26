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

#include <include/concurrency/transaction_manager_factory.h>
#include "network/connection_handle.h"
#include "parser/postgresparser.h"
#include "parser/sql_statement.h"
#include "common/statement_cache.h"
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
  // Default database name
  int rows_affected_;
  // The optimizer used for this connection
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer_;
  // flag of single statement txn
  bool single_statement_txn_;
  std::vector<ResultValue> result_;
  std::stack<TcopTxnState> tcop_txn_state_;
  executor::ExecutionResult p_status_;
  StatementCache statement_cache_;
};

// Execute a statement
ResultType ExecuteStatement(
    ClientProcessState &state,
    const std::shared_ptr<Statement> &statement,
    const std::vector<type::Value> &params, bool unnamed,
    std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,
    const std::vector<int> &result_format, std::vector<ResultValue> &result,
    size_t thread_id = 0);

// Helper to handle txn-specifics for the plan-tree of a statement.
executor::ExecutionResult ExecuteHelper(
    std::shared_ptr<planner::AbstractPlan> plan,
    const std::vector<type::Value> &params, std::vector<ResultValue> &result,
    const std::vector<int> &result_format, size_t thread_id = 0);

// Prepare a statement using the parse tree
std::shared_ptr<Statement> PrepareStatement(
    ClientProcessState &state,
    const std::string &statement_name, const std::string &query_string,
    std::unique_ptr<parser::SQLStatementList> sql_stmt_list,
    size_t thread_id = 0);

bool BindParamsForCachePlan(
    ClientProcessState &state,
    const std::vector<std::unique_ptr<expression::AbstractExpression>> &,
    size_t thread_id = 0);

std::vector<FieldInfo> GenerateTupleDescriptor(
    parser::SQLStatement *select_stmt);

FieldInfo GetColumnFieldForValueType(std::string column_name,
                                     type::TypeId column_type);

ResultType CommitQueryHelper();

void ExecuteStatementPlanGetResult();

ResultType ExecuteStatementGetResult();

void ProcessInvalidStatement(ClientProcessState &state);

ResultType BeginQueryHelper(size_t thread_id);

ResultType AbortQueryHelper();

// Get all data tables from a TableRef.
// For multi-way join
// still a HACK
void GetTableColumns(parser::TableRef *from_table,
                     std::vector<catalog::Column> &target_tables);

} // namespace tcop
} // namespace peloton