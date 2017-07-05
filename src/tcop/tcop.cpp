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
#include "optimizer/simple_optimizer.h"
#include "common/exception.h"
#include "parser/select_statement.h"

#include "catalog/catalog.h"
#include "executor/plan_executor.h"
#include "optimizer/simple_optimizer.h"
#include "optimizer/optimizer.h"

#include "planner/plan_util.h"

#include <boost/algorithm/string.hpp>
#include <include/parser/postgresparser.h>

#define NEW_OPTIMIZER

namespace peloton {
namespace tcop {

  TrafficCop::TrafficCop() {
    LOG_TRACE("Starting a new TrafficCop");
#ifdef NEW_OPTIMIZER
    optimizer_.reset(new optimizer::Optimizer);
#else
    optimizer_.reset(new optimizer::SimpleOptimizer());
#endif
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

//ResultType TrafficCop::BeginQueryHelper(const size_t thread_id) {
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto txn = txn_manager.BeginTransaction(thread_id);
//
//  // this shouldn't happen
//  if (txn == nullptr) {
//    LOG_DEBUG("Begin txn failed");
//    return ResultType::FAILURE;
//  }
//
//  // initialize the current result as success
//  tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
//  return ResultType::SUCCESS;
//}

  ResultType TrafficCop::CommitQueryHelper() {
    LOG_INFO("before commit txn id: %lu", tcop_txn_state_.top().first->GetTransactionId());
    // do nothing if we have no active txns
    if (tcop_txn_state_.empty()) return ResultType::NOOP;
    auto &curr_state = tcop_txn_state_.top();
    tcop_txn_state_.pop();
    // commit the txn only if it has not aborted already
    if (curr_state.second != ResultType::ABORTED) {
      LOG_INFO("ENTER != aborted");
      auto txn = curr_state.first;
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto result = txn_manager.CommitTransaction(txn);
      // # 623
      return result;
    } else {
      // otherwise, the txn has already been aborted
      LOG_INFO("final tcop_txn_state_ size() %lu", tcop_txn_state_.size());
      auto txn = curr_state.first;
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto result = txn_manager.AbortTransaction(txn);
      // # 623
      return result;
//      return ResultType::ABORTED;
    }
  }

  ResultType TrafficCop::AbortQueryHelper() {
    
    LOG_INFO("ENTER NOT SINGLE ABORTQUERYHELPER");
    // do nothing if we have no active txns
    if (tcop_txn_state_.empty()) return ResultType::NOOP;
    LOG_INFO("AFTER NOT SINGLE ABORTQUERYHELPER");
    auto &curr_state = tcop_txn_state_.top();
    tcop_txn_state_.pop();
    // explicitly abort the txn only if it has not aborted already
    if (curr_state.second != ResultType::ABORTED) {
      LOG_INFO("SINGLE ABORTQUERYHELPER");
      auto txn = curr_state.first;
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto result = txn_manager.AbortTransaction(txn);
      return result;
    } else {
      LOG_INFO("SINGLE ABORTQUERYHELPER!!");
      // otherwise, the txn has already been aborted
      return ResultType::ABORTED;
    }
  }

  ResultType TrafficCop::ExecuteStatement(
      const std::string &query, std::vector<StatementResult> &result,
      std::vector<FieldInfo> &tuple_descriptor, int &rows_changed,
      std::string &error_message, const size_t thread_id UNUSED_ATTRIBUTE) {
    LOG_INFO("Received %s", query.c_str());

    std::string unnamed_statement = "unnamed";
    // # 623
    std::shared_ptr<Statement> statement(new Statement(unnamed_statement, query));
    // transaction
    // --statements except BEGIN in a transaction
    // --
    if (!tcop_txn_state_.empty()) {
      LOG_INFO("TCOP_TXN_STATE NUMBER: %lu", tcop_txn_state_.size());
      single_statement_txn = false;
      auto &curr_state = tcop_txn_state_.top();
      LOG_INFO("TOP TRANSACTION ID: %lu", curr_state.first->GetTransactionId());
      // prepare statement in txn only if it has not aborted already
      if (curr_state.second != ResultType::ABORTED) {
        // this statement is not aborted, then execute this statement
        statement = PrepareStatement(unnamed_statement, query, error_message);
        if (statement.get() == nullptr) {
          LOG_INFO("TRANSACTION IS GONNA BE ABORTED!!!");
          // first mark this transaction aborted
          tcop_txn_state_.top().second = ResultType::ABORTED;
          return ResultType::ABORTED;
        }
      } else {
        // otherwise, the txn has already been aborted
        if (query == "COMMIT" || query == "ROLLBACK") {
          // commit aborted txn
          return CommitQueryHelper();
        } else {
          // ignore following statements after first aborted statement
          LOG_INFO("TRANSACTION ABORTED!!!");
          return ResultType::ABORTED;
        }
      }
    } else {
      // --BEGIN
      // --single-statement transaction
      // ----prepareStatement abort
      if (query == "BEGIN") { // only begin a new transaction
        // note this transaction is not single-statement transaction
        single_statement_txn = false;
        auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
        auto txn = txn_manager.BeginTransaction(thread_id);
        // pass txn handle to optimizer_
        (*optimizer_).consistentTxn = txn;

        LOG_INFO("after begin txn id: %lu", txn->GetTransactionId());

        // this shouldn't happen
        if (txn == nullptr) {
          LOG_DEBUG("Begin txn failed");
        }
        // initialize the current result as success
        tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
        return ResultType::SUCCESS;

      } else {
        // single statement
        single_statement_txn = true;
        auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
        auto txn = txn_manager.BeginTransaction(thread_id);
        // this shouldn't happen
        if (txn == nullptr) {
          LOG_DEBUG("Begin txn failed");
        }
        (*optimizer_).consistentTxn = txn;
        LOG_INFO("after begin single txn id: %lu", txn->GetTransactionId());
        tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
        statement = PrepareStatement(unnamed_statement, query, error_message);
        // prepareStatement ABORT, catalog throw exception
        // create dup table, drop table not existed
        if (statement.get() == nullptr) {
          // reset tcop_txn_state_
          LOG_INFO("SINGLE ABORT!");
          return AbortQueryHelper();
//          return ResultType::ABORTED;
        }
        LOG_INFO("SINGLE NOT ABORT!");

      }
    }


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
      LOG_INFO("Execution succeeded!");
      tuple_descriptor = std::move(statement->GetTupleDescriptor());
    } else {
      LOG_INFO("Execution failed!");
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
    LOG_INFO("Execute Statement of query: %s",
             statement->GetQueryString().c_str());
    LOG_TRACE("Execute Statement Plan:\n%s",
              planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
    LOG_INFO("STATS %d", tcop_txn_state_.top().second);

    try {
      switch(statement->GetQueryType()) {
        case QueryType::QUERY_BEGIN:
          return ResultType::SUCCESS;
        case QueryType::QUERY_COMMIT:
          return CommitQueryHelper();
        case QueryType::QUERY_ROLLBACK:
        LOG_INFO("abort!!!");
          tcop_txn_state_.top().second = ResultType::ABORTED;
          return CommitQueryHelper();
//          return AbortQueryHelper();
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
      const size_t thread_id UNUSED_ATTRIBUTE) {
    concurrency::Transaction *txn;
    bool init_failure = false;
    executor::ExecuteResult p_status;
    // # 623
    auto &curr_state = GetCurrentTxnState();
    txn = curr_state.first;

    LOG_INFO("HELLO %lu", txn->GetTransactionId());

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
      LOG_INFO("::::::::::::::::::: %d", txn_result);
      if (!tcop_txn_state_.empty()) {
        LOG_INFO("+++++++++++++++++++ %d", tcop_txn_state_.top().second);
      }

      if (single_statement_txn == true || init_failure == true ||
          txn_result == ResultType::FAILURE) {
//        auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

        LOG_INFO(
            "About to commit: single stmt: %d, init_failure: %d, txn_result: %s",
            single_statement_txn, init_failure,
            ResultTypeToString(txn_result).c_str());
        switch (txn_result) {
          case ResultType::SUCCESS:
            // Commit
          LOG_INFO("Commit Transaction");

//            p_status.m_result = txn_manager.CommitTransaction(txn);
            p_status.m_result = CommitQueryHelper();
            break;

          case ResultType::FAILURE:
          default:
            // Abort
          LOG_INFO("Abort Transaction");
            if (single_statement_txn == true) {
              LOG_INFO("NUMBER %lu", tcop_txn_state_.size());
              p_status.m_result = AbortQueryHelper();
            } else {
              tcop_txn_state_.top().second = ResultType::ABORTED;
              p_status.m_result = ResultType::ABORTED;
            }
        }
      }
    } else {
      // otherwise, we have already aborted
      p_status.m_result = ResultType::ABORTED;
    }
    // # 623
//    if (single_statement_txn == true) {
//      LOG_INFO("NUMBER %lu", tcop_txn_state_.size());
//      p_status.m_result = AbortQueryHelper();
//    } else {
//      tcop_txn_state_.top().second = ResultType::ABORTED;
//    }
    LOG_INFO("__________________ %lu", tcop_txn_state_.size());
    return p_status;
  }

  std::shared_ptr<Statement> TrafficCop::PrepareStatement(
      const std::string &statement_name, const std::string &query_string,
      UNUSED_ATTRIBUTE std::string &error_message) {
    LOG_TRACE("Prepare Statement name: %s", statement_name.c_str());
    LOG_INFO("Prepare Statement query: %s", query_string.c_str());

    std::shared_ptr<Statement> statement(
        new Statement(statement_name, query_string));
    try {

      auto &peloton_parser = parser::PostgresParser::GetInstance();
      auto sql_stmt = peloton_parser.BuildParseTree(query_string);
      if (sql_stmt->is_valid == false) {
        throw ParserException("Error parsing SQL statement");
      }
      LOG_INFO("Prepare Statement query 2");
      auto plan = optimizer_->BuildPelotonPlanTree(sql_stmt);
      LOG_INFO("Prepare Statement query 3");
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
      return std::move(statement);
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
                from_table->GetDatabaseName(), from_table->GetTableName(), GetCurrentTxnState().first));
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
      std::string column_name, type::Type::TypeId column_type) {
    PostgresValueType field_type;
    size_t field_size;
    switch (column_type) {
      case type::Type::BOOLEAN:
      case type::Type::TINYINT: {
        field_type = PostgresValueType::BOOLEAN;
        field_size = 1;
        break;
      }
      case type::Type::SMALLINT: {
        field_type = PostgresValueType::SMALLINT;
        field_size = 2;
        break;
      }
      case type::Type::INTEGER: {
        field_type = PostgresValueType::INTEGER;
        field_size = 4;
        break;
      }
      case type::Type::BIGINT: {
        field_type = PostgresValueType::BIGINT;
        field_size = 8;
        break;
      }
      case type::Type::DECIMAL: {
        field_type = PostgresValueType::DOUBLE;
        field_size = 8;
        break;
      }
      case type::Type::VARCHAR:
      case type::Type::VARBINARY: {
        field_type = PostgresValueType::TEXT;
        field_size = 255;
        break;
      }
      case type::Type::TIMESTAMP: {
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
