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
#include "type/type.h"
#include "type/types.h"

#include "configuration/configuration.h"

#include "expression/aggregate_expression.h"
#include "expression/expression_util.h"
#include "common/exception.h"
#include "parser/select_statement.h"

#include "catalog/catalog.h"
#include "executor/plan_executor.h"
#include "optimizer/optimizer.h"
#include "planner/plan_util.h"

#include <boost/algorithm/string.hpp>
#include <include/parser/postgresparser.h>


namespace peloton {
namespace tcop {

TrafficCop::TrafficCop() {
  LOG_TRACE("Starting a new TrafficCop");
  optimizer_.reset(new optimizer::Optimizer);
}

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
TrafficCop &TrafficCop::GetInstance() {
  static TrafficCop tcop;
  tcop.Reset();
  return tcop;
}

TrafficCop::TcopTxnState &TrafficCop::GetDefaultTxnState() {
  static TcopTxnState default_state;
  default_state = std::make_pair(nullptr, ResultType::INVALID);
  return default_state;
}

TrafficCop::TcopTxnState &TrafficCop::GetCurrentTxnState() {
  if (tcop_txn_state_.empty()) {
    return GetDefaultTxnState();
  }
  return tcop_txn_state_.top();
}

ResultType TrafficCop::BeginQueryHelper(const size_t thread_id) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction(thread_id);

  // this shouldn't happen
  if (txn == nullptr) {
    LOG_DEBUG("Begin txn failed");
    return ResultType::FAILURE;
  }

  // initialize the current result as success
  tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  return ResultType::SUCCESS;
}

ResultType TrafficCop::CommitQueryHelper() {
  // do nothing if we have no active txns
  if (tcop_txn_state_.empty()) return ResultType::NOOP;
  auto &curr_state = tcop_txn_state_.top();
  tcop_txn_state_.pop();
  // commit the txn only if it has not aborted already
  if (curr_state.second != ResultType::ABORTED) {
    auto txn = curr_state.first;
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto result = txn_manager.CommitTransaction(txn);
    return result;
  } else {
    // otherwise, the txn has already been aborted
    return ResultType::ABORTED;
  }
}

ResultType TrafficCop::AbortQueryHelper() {
  // do nothing if we have no active txns
  if (tcop_txn_state_.empty()) return ResultType::NOOP;
  auto &curr_state = tcop_txn_state_.top();
  tcop_txn_state_.pop();
  // explicitly abort the txn only if it has not aborted already
  if (curr_state.second != ResultType::ABORTED) {
    auto txn = curr_state.first;
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto result = txn_manager.AbortTransaction(txn);
    return result;
  } else {
    // otherwise, the txn has already been aborted
    return ResultType::ABORTED;
  }
}

ResultType TrafficCop::ExecuteStatement(
    const std::string &query, std::vector<StatementResult> &result,
    std::vector<FieldInfo> &tuple_descriptor, int &rows_changed,
    std::string &error_message, const size_t thread_id UNUSED_ATTRIBUTE) {
  LOG_TRACE("Received %s", query.c_str());

  // Prepare the statement
  std::string unnamed_statement = "unnamed";
  auto statement = PrepareStatement(unnamed_statement, query, error_message);

  if (statement.get() == nullptr) {
    return ResultType::FAILURE;
  }

  // Then, execute the statement
  bool unnamed = true;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  std::vector<type::Value> params;
  auto status =
      ExecuteStatement(statement, params, unnamed, nullptr, result_format,
                       result, rows_changed, error_message, thread_id);

  if (status == ResultType::SUCCESS) {
    LOG_TRACE("Execution succeeded!");
    tuple_descriptor = statement->GetTupleDescriptor();
  } else {
    LOG_TRACE("Execution failed!");
  }

  return status;
}

ResultType TrafficCop::ExecuteStatement(
    const std::shared_ptr<Statement> &statement,
    const std::vector<type::Value> &params, UNUSED_ATTRIBUTE const bool unnamed,
    std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,
    const std::vector<int> &result_format, std::vector<StatementResult> &result,
    int &rows_changed, UNUSED_ATTRIBUTE std::string &error_message,
    const size_t thread_id UNUSED_ATTRIBUTE) {
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
    switch(statement->GetQueryType()) {
      case QueryType::QUERY_BEGIN:
        return BeginQueryHelper(thread_id);
      case QueryType::QUERY_COMMIT:
        return CommitQueryHelper();
      case QueryType::QUERY_ROLLBACK:
        return AbortQueryHelper();
      default:
        auto status = ExecuteStatementPlan(statement->GetPlanTree().get(), params,
                                           result, result_format,
                                           thread_id);
        LOG_TRACE("Statement executed. Result: %s",
                  ResultTypeToString(status.m_result).c_str());
        rows_changed = status.m_processed;
        return status.m_result;
    }
  } catch (Exception &e) {
    error_message = e.what();
    return ResultType::FAILURE;
  }
}

executor::ExecuteResult TrafficCop::ExecuteStatementPlan(
    const planner::AbstractPlan *plan, const std::vector<type::Value> &params,
    std::vector<StatementResult> &result, const std::vector<int> &result_format,
    const size_t thread_id) {
  concurrency::Transaction *txn;
  bool single_statement_txn = false, init_failure = false;
  executor::ExecuteResult p_status;

  auto &curr_state = GetCurrentTxnState();
  if (tcop_txn_state_.empty()) {
    // no active txn, single-statement txn
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // new txn, reset result status
    curr_state.second = ResultType::SUCCESS;
    txn = txn_manager.BeginTransaction(thread_id);
    single_statement_txn = true;
  } else {
    // get ptr to current active txn
    txn = curr_state.first;
  }

  // skip if already aborted
  if (curr_state.second != ResultType::ABORTED) {
    PL_ASSERT(txn);
    p_status = executor::PlanExecutor::ExecutePlan(plan, txn, params, result,
                                                   result_format);

    if (p_status.m_result == ResultType::FAILURE) {
      // only possible if init failed
      init_failure = true;
    }

    auto txn_result = txn->GetResult();
    if (single_statement_txn == true || init_failure == true ||
        txn_result == ResultType::FAILURE) {
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

      LOG_TRACE(
          "About to commit: single stmt: %d, init_failure: %d, txn_result: %s",
          single_statement_txn, init_failure,
          ResultTypeToString(txn_result).c_str());
      switch (txn_result) {
        case ResultType::SUCCESS:
          // Commit
          LOG_TRACE("Commit Transaction");
          p_status.m_result = txn_manager.CommitTransaction(txn);
          break;

        case ResultType::FAILURE:
        default:
          // Abort
          LOG_TRACE("Abort Transaction");
          p_status.m_result = txn_manager.AbortTransaction(txn);
          curr_state.second = ResultType::ABORTED;
      }
    }
  } else {
    // otherwise, we have already aborted
    p_status.m_result = ResultType::ABORTED;
  }
  return p_status;
}

std::shared_ptr<Statement> TrafficCop::PrepareStatement(
    const std::string &statement_name, const std::string &query_string,
    UNUSED_ATTRIBUTE std::string &error_message) {
  LOG_TRACE("Prepare Statement name: %s", statement_name.c_str());
  LOG_TRACE("Prepare Statement query: %s", query_string.c_str());

  std::shared_ptr<Statement> statement(
      new Statement(statement_name, query_string));
  try {
    auto &peloton_parser = parser::PostgresParser::GetInstance();
    auto sql_stmt = peloton_parser.BuildParseTree(query_string);
    if (sql_stmt->is_valid == false) {
      throw ParserException("Error parsing SQL statement");
    }
    auto plan = optimizer_->BuildPelotonPlanTree(sql_stmt);
    statement->SetPlanTree(plan);

    // Get the tables that our plan references so that we know how to
    // invalidate it at a later point when the catalog changes
    const std::set<oid_t> table_oids =
        planner::PlanUtil::GetTablesReferenced(plan.get());
    statement->SetReferencedTables(table_oids);

    for (auto stmt : sql_stmt->GetStatements()) {
      LOG_TRACE("SQLStatement: %s", stmt->GetInfo().c_str());
      if (stmt->GetType() == StatementType::SELECT) {
        auto tuple_descriptor = GenerateTupleDescriptor(stmt);
        statement->SetTupleDescriptor(tuple_descriptor);
      }
      break;
    }

#ifdef LOG_DEBUG_ENABLED
    if (statement->GetPlanTree().get() != nullptr) {
      LOG_DEBUG("Statement Prepared: %s", statement->GetInfo().c_str());
      LOG_TRACE("%s", statement->GetPlanTree().get()->GetInfo().c_str());
    }
#endif
    return statement;
  } catch (Exception &e) {
    error_message = e.what();
    return nullptr;
  }
}

void TrafficCop::GetDataTables(
    parser::TableRef *from_table,
    std::vector<storage::DataTable *> &target_tables) {
  if (from_table == nullptr) return;

  if (from_table->list == NULL) {
    if (from_table->join == NULL) {
      auto *target_table = static_cast<storage::DataTable *>(
          catalog::Catalog::GetInstance()->GetTableWithName(
              from_table->GetDatabaseName(), from_table->GetTableName()));
      target_tables.push_back(target_table);
    } else {
      GetDataTables(from_table->join->left, target_tables);
      GetDataTables(from_table->join->right, target_tables);
    }
  }

  // Query has multiple tables. Recursively add all tables
  else {
    for (auto table : *(from_table->list)) {
      GetDataTables(table, target_tables);
    }
  }
}

std::vector<FieldInfo> TrafficCop::GenerateTupleDescriptor(
    parser::SQLStatement *sql_stmt) {
  std::vector<FieldInfo> tuple_descriptor;
  if (sql_stmt->GetType() != StatementType::SELECT) return tuple_descriptor;
  auto select_stmt = (parser::SelectStatement *)sql_stmt;

  // TODO: this is a hack which I don't have time to fix now
  // but it replaces a worse hack that was here before
  // What should happen here is that plan nodes should store
  // the schema of their expected results and here we should just read
  // it and put it in the tuple descriptor

  // Get the columns information and set up
  // the columns description for the returned results
  // Set up the table
  std::vector<storage::DataTable *> target_tables;

  // Check if query only has one Table
  // Example : SELECT * FROM A;
  GetDataTables(select_stmt->from_table, target_tables);

  int count = 0;
  for (auto expr : *select_stmt->select_list) {
    count++;
    if (expr->GetExpressionType() == ExpressionType::STAR) {
      for (auto target_table : target_tables) {
        // Get the columns of the table
        auto &table_columns = target_table->GetSchema()->GetColumns();
        for (auto column : table_columns) {
          tuple_descriptor.push_back(
              GetColumnFieldForValueType(column.GetName(), column.GetType()));
        }
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

FieldInfo TrafficCop::GetColumnFieldForValueType(
    std::string column_name, type::TypeId column_type) {
  PostgresValueType field_type;
  size_t field_size;
  switch (column_type) {
    case type::TypeId::BOOLEAN:
    case type::TypeId::TINYINT: {
      field_type = PostgresValueType::BOOLEAN;
      field_size = 1;
      break;
    }
    case type::TypeId::SMALLINT: {
      field_type = PostgresValueType::SMALLINT;
      field_size = 2;
      break;
    }
    case type::TypeId::INTEGER: {
      field_type = PostgresValueType::INTEGER;
      field_size = 4;
      break;
    }
    case type::TypeId::BIGINT: {
      field_type = PostgresValueType::BIGINT;
      field_size = 8;
      break;
    }
    case type::TypeId::DECIMAL: {
      field_type = PostgresValueType::DOUBLE;
      field_size = 8;
      break;
    }
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY: {
      field_type = PostgresValueType::TEXT;
      field_size = 255;
      break;
    }
    case type::TypeId::TIMESTAMP: {
      field_type = PostgresValueType::TIMESTAMPS;
      field_size = 64;  // FIXME: Bytes???
      break;
    }
    default: {
      // Type not Identified
      LOG_ERROR("Unrecognized field type '%s' [%d] for field '%s'",
                TypeIdToString(column_type).c_str(), column_type,
                column_name.c_str());
      field_type = PostgresValueType::TEXT;
      field_size = 255;
      break;
    }
  }
  // HACK: Convert the type into a oid_t
  // This ugly and I don't like it one bit...
  return std::make_tuple(column_name, static_cast<oid_t>(field_type),
                         field_size);
}

FieldInfo TrafficCop::GetColumnFieldForAggregates(std::string name,
                                                  ExpressionType expr_type) {
  // For now we only return INT for (MAX , MIN)
  // TODO: Check if column type is DOUBLE and return it for (MAX. MIN)

  PostgresValueType field_type;
  size_t field_size;
  std::string field_name;

  // Check the expression type and return the corresponding description
  switch (expr_type) {
    case ExpressionType::AGGREGATE_MAX:
    case ExpressionType::AGGREGATE_MIN:
    case ExpressionType::AGGREGATE_COUNT: {
      field_type = PostgresValueType::INTEGER;
      field_size = 4;
      field_name = name;
      break;
    }
    // Return a DOUBLE if the functiob is AVG
    case ExpressionType::AGGREGATE_AVG: {
      field_type = PostgresValueType::DOUBLE;
      field_size = 8;
      field_name = name;
      break;
    }
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      field_type = PostgresValueType::INTEGER;
      field_size = 4;
      field_name = "COUNT(*)";
      break;
    }
    default: {
      field_type = PostgresValueType::TEXT;
      field_size = 255;
      field_name = name;
    }
  }  // SWITCH
  return std::make_tuple(field_name, static_cast<oid_t>(field_type),
                         field_size);
}

}  // End tcop namespace
}  // End peloton namespace
