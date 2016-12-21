//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tcop.cpp
//
// Identification: src/tcop/tcop.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "tcop/tcop.h"

#include "catalog/catalog.h"
#include "common/abstract_tuple.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/portal.h"
#include "configuration/configuration.h"
#include "executor/plan_executor.h"
#include "expression/aggregate_expression.h"
#include "expression/expression_util.h"
#include "optimizer/simple_optimizer.h"
#include "parser/parser.h"
#include "parser/select_statement.h"
#include "planner/plan_util.h"
#include "type/type.h"
#include "type/types.h"

#include <boost/algorithm/string.hpp>

namespace peloton {
namespace tcop {

TrafficCop::TrafficCop() { optimizer_.reset(new optimizer::SimpleOptimizer()); }

void TrafficCop::Reset() {
  std::stack<TcopTxnState> new_tcop_txn_state;
  // clear out the stack
  swap(tcop_txn_state_, new_tcop_txn_state);
  optimizer_->Reset();
}

TrafficCop::~TrafficCop() {
  // Abort all running transactions
  while (tcop_txn_state_.empty() == false) {
    AbortQueryHelper();
  }
}

/* Singleton accessor
 * NOTE: Used by in unit tests ONLY
 */
TrafficCop& TrafficCop::GetInstance() {
  static TrafficCop tcop;
  tcop.Reset();
  return tcop;
}

TrafficCop::TcopTxnState& TrafficCop::GetDefaultTxnState() {
  static TcopTxnState default_state;
  default_state = std::make_pair(nullptr, Result::RESULT_INVALID);
  return default_state;
}

TrafficCop::TcopTxnState& TrafficCop::GetCurrentTxnState() {
  if (tcop_txn_state_.empty()) {
    return GetDefaultTxnState();
  }
  return tcop_txn_state_.top();
}

Result TrafficCop::BeginQueryHelper() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // this shouldn't happen
  if (txn == nullptr){
    LOG_DEBUG("Begin txn failed");
    return Result::RESULT_FAILURE;
  }

  // initialize the current result as success
  tcop_txn_state_.emplace(txn, Result::RESULT_SUCCESS);
  return Result::RESULT_SUCCESS;
}

Result TrafficCop::CommitQueryHelper() {
  // do nothing if we have no active txns
  if (tcop_txn_state_.empty())
    return Result::RESULT_NOOP;
  auto &curr_state = tcop_txn_state_.top();
  tcop_txn_state_.pop();
  // commit the txn only if it has not aborted already
  if (curr_state.second != Result::RESULT_ABORTED) {
    auto txn = curr_state.first;
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto result = txn_manager.CommitTransaction(txn);
    return result;
  } else {
    // otherwise, the txn has already been aborted
    return Result::RESULT_ABORTED;
  }
}

Result TrafficCop::AbortQueryHelper() {
  // do nothing if we have no active txns
  if (tcop_txn_state_.empty())
    return Result::RESULT_NOOP;
  auto &curr_state = tcop_txn_state_.top();
  tcop_txn_state_.pop();
  // explicitly abort the txn only if it has not aborted already
  if (curr_state.second != Result::RESULT_ABORTED) {
    auto txn = curr_state.first;
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto result = txn_manager.AbortTransaction(txn);
    return result;
  } else {
    // otherwise, the txn has already been aborted
    return Result::RESULT_ABORTED;
  }
}

Result TrafficCop::ExecuteStatement(
    const std::string &query, std::vector<ResultType> &result,
    std::vector<FieldInfoType> &tuple_descriptor, int &rows_changed,
    std::string &error_message) {
  LOG_TRACE("Received %s", query.c_str());

  // Prepare the statement
  std::string unnamed_statement = "unnamed";
  auto statement = PrepareStatement(unnamed_statement, query, error_message);

  if (statement.get() == nullptr) {
    return Result::RESULT_FAILURE;
  }

  // Then, execute the statement
  bool unnamed = true;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  std::vector<type::Value> params;
  auto status =
      ExecuteStatement(statement, params, unnamed, nullptr, result_format,
                       result, rows_changed, error_message);

  if (status == Result::RESULT_SUCCESS) {
    LOG_TRACE("Execution succeeded!");
    tuple_descriptor = std::move(statement->GetTupleDescriptor());
  } else {
    LOG_TRACE("Execution failed!");
  }

  return status;
}

Result TrafficCop::ExecuteStatement(
    const std::shared_ptr<Statement> &statement,
    const std::vector<type::Value> &params, UNUSED_ATTRIBUTE const bool unnamed,
    std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,
    const std::vector<int> &result_format, std::vector<ResultType> &result,
    int &rows_changed, UNUSED_ATTRIBUTE std::string &error_message) {

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->InitQueryMetric(statement,
                                                               param_stats);
  }
  LOG_TRACE("Execute Statement of name: %s",
            statement->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s",
            statement->GetStatementName().c_str());
  LOG_TRACE("Execute Statement Plan:\n%s",
            planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());

  try {
    auto query_str = boost::to_upper_copy<std::string>(statement->GetQueryString());
    if (query_str == "BEGIN")
      return BeginQueryHelper();
    else if (query_str == "COMMIT")
      return CommitQueryHelper();
    else if (query_str == "ABORT")
      return AbortQueryHelper();
    else {
      auto status = ExecuteStatementPlan(statement->GetPlanTree().get(), params,
                              result, result_format);
      LOG_TRACE("Statement executed. Result: %d", status.m_result);
      rows_changed = status.m_processed;
      return status.m_result;
    }
  }
  catch (Exception &e) {
    error_message = e.what();
    return Result::RESULT_FAILURE;
  }
}

bridge::peloton_status
  TrafficCop::ExecuteStatementPlan(const planner::AbstractPlan *plan,
                               const std::vector<type::Value> &params,
                               std::vector<ResultType> &result,
                               const std::vector<int> &result_format) {
  concurrency::Transaction *txn;
  bool single_statement_txn = false, init_failure = false;
  bridge::peloton_status p_status;

  auto &curr_state = GetCurrentTxnState();
  if (tcop_txn_state_.empty()) {
    // no active txn, single-statement txn
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // new txn, reset result status
    curr_state.second = Result::RESULT_SUCCESS;
    txn = txn_manager.BeginTransaction();
    single_statement_txn = true;
  } else {
    // get ptr to current active txn
    txn = curr_state.first;
  }

  // skip if already aborted
  if (curr_state.second != Result::RESULT_ABORTED) {
    PL_ASSERT(txn);
    p_status = bridge::PlanExecutor::ExecutePlan(plan, txn, params, result,
                                                 result_format);

    if (p_status.m_result == Result::RESULT_FAILURE) {
      // only possible if init failed
      init_failure = true;
    }

    auto txn_result = txn->GetResult();
    if (single_statement_txn == true || init_failure == true ||
        txn_result == Result::RESULT_FAILURE) {
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

      LOG_TRACE("About to commit: single stmt: %d, init_failure: %d, txn_result: %d",
                single_statement_txn, init_failure, txn_result);
      switch (txn_result) {
        case Result::RESULT_SUCCESS:
          // Commit
          LOG_TRACE("Commit Transaction");
          p_status.m_result = txn_manager.CommitTransaction(txn);
          break;

        case Result::RESULT_FAILURE:
        default:
          // Abort
          LOG_TRACE("Abort Transaction");
          p_status.m_result = txn_manager.AbortTransaction(txn);
          curr_state.second = Result::RESULT_ABORTED;
      }
    }
  } else {
    // otherwise, we have already aborted
    p_status.m_result = Result::RESULT_ABORTED;
  }
  return p_status;
}

std::shared_ptr<Statement> TrafficCop::PrepareStatement(
    const std::string &statement_name, const std::string &query_string,
    UNUSED_ATTRIBUTE std::string &error_message) {
  std::shared_ptr<Statement> statement;

  LOG_DEBUG("Prepare Statement name: %s", statement_name.c_str());
  LOG_DEBUG("Prepare Statement query: %s", query_string.c_str());

  statement.reset(new Statement(statement_name, query_string));
  try {
    auto &peloton_parser = parser::Parser::GetInstance();
    auto sql_stmt = peloton_parser.BuildParseTree(query_string);
    if (sql_stmt->is_valid == false) {
      throw ParserException("Error parsing SQL statement");
    }
    statement->SetPlanTree(optimizer_->BuildPelotonPlanTree(sql_stmt));

    for (auto stmt : sql_stmt->GetStatements()) {
      if (stmt->GetType() == STATEMENT_TYPE_SELECT) {
        auto tuple_descriptor = GenerateTupleDescriptor(stmt);
        statement->SetTupleDescriptor(tuple_descriptor);
      }
      break;
    }
#ifdef LOG_TRACE_ENABLED
    if (statement->GetPlanTree().get() != nullptr) {
      LOG_TRACE("Statement Prepared\n%s",
                statement->GetPlanTree().get()->GetInfo().c_str());
    }
#endif
    return std::move(statement);
  } catch (Exception &e) {
    error_message = e.what();
    return nullptr;
  }
}

std::vector<FieldInfoType> TrafficCop::GenerateTupleDescriptor(
    parser::SQLStatement *stmt) {
  std::vector<FieldInfoType> tuple_descriptor;
  if (stmt->GetType() != STATEMENT_TYPE_SELECT) return tuple_descriptor;
  auto select_stmt = (parser::SelectStatement *)stmt;

  // TODO: this is a hack which I don't have time to fix now
  // but it replaces a worse hack that was here before
  // What should happen here is that plan nodes should store
  // the schema of their expected results and here we should just read
  // it and put it in the tuple descriptor

  // Get the columns information and set up
  // the columns description for the returned results
  // Set up the table
  storage::DataTable *target_table = nullptr;

  // Check if query only has one Table
  // Example : SELECT * FROM A;
  if (select_stmt->from_table->list == NULL) {
    target_table = static_cast<storage::DataTable *>(
        catalog::Catalog::GetInstance()->GetTableWithName(
            select_stmt->from_table->GetDatabaseName(),
            select_stmt->from_table->GetTableName()));
  }

  // Query has multiple tables
  // Example: SELECT COUNT(ID) FROM A,B <Condition>
  // For now we only pick the first table in the list
  // FIX: Better handle for queries with multiple tables
  else {
    for (auto table : *select_stmt->from_table->list) {
      target_table = static_cast<storage::DataTable *>(
          catalog::Catalog::GetInstance()->GetTableWithName(
              table->GetDatabaseName(), table->GetTableName()));
      break;
    }
  }

  // Get the columns of the table
  auto &table_columns = target_table->GetSchema()->GetColumns();

  int count = 0;
  for (auto expr : *select_stmt->select_list) {
    count++;
    if (expr->GetExpressionType() == EXPRESSION_TYPE_STAR) {
      for (auto column : table_columns) {
        tuple_descriptor.push_back(
            GetColumnFieldForValueType(column.column_name, column.column_type));
      }
    } else {
      std::string col_name;
      if (expr->alias.empty()) {
        col_name = expr->expr_name_.empty()
                       ? std::string("expr") + std::to_string(count)
                       : expr->expr_name_;
      } else {
        col_name = expr->alias;
      }
      tuple_descriptor.push_back(
          GetColumnFieldForValueType(col_name, expr->GetValueType()));
    }
  }
  return tuple_descriptor;
}

FieldInfoType TrafficCop::GetColumnFieldForValueType(
    std::string column_name, type::Type::TypeId column_type) {
  switch (column_type) {
    case type::Type::INTEGER:
      return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_INTEGER, 4);
    case type::Type::DECIMAL:
      return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_DOUBLE, 8);
    case type::Type::VARCHAR:
    case type::Type::VARBINARY:
      return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_TEXT, 255);
    case type::Type::TIMESTAMP:
      return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_TIMESTAMPS, 64);
    default:
      // Type not Identified
      LOG_ERROR("Unrecognized column type '%s' [%d] for column '%s'",
                TypeIdToString(column_type).c_str(), column_type,
                column_name.c_str());
  }
  // return String
  return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_TEXT, 255);
}

FieldInfoType TrafficCop::GetColumnFieldForAggregates(
    std::string name, ExpressionType expr_type) {
  // For now we only return INT for (MAX , MIN)
  // TODO: Check if column type is DOUBLE and return it for (MAX. MIN)

  // Check the expression type and return the corresponding description
  if (expr_type == EXPRESSION_TYPE_AGGREGATE_MAX ||
      expr_type == EXPRESSION_TYPE_AGGREGATE_MIN ||
      expr_type == EXPRESSION_TYPE_AGGREGATE_COUNT) {
    return std::make_tuple(name, POSTGRES_VALUE_TYPE_INTEGER, 4);
  }

  // Return double if function is AVERAGE
  if (expr_type == EXPRESSION_TYPE_AGGREGATE_AVG) {
    return std::make_tuple(name, POSTGRES_VALUE_TYPE_DOUBLE, 8);
  }

  if (expr_type == EXPRESSION_TYPE_AGGREGATE_COUNT_STAR) {
    return std::make_tuple("COUNT(*)", POSTGRES_VALUE_TYPE_INTEGER, 4);
  }

  return std::make_tuple(name, POSTGRES_VALUE_TYPE_TEXT, 255);
}
}  // End tcop namespace
}  // End peloton namespace
