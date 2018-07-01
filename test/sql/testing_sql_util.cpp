//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// testing_sql_util.cpp
//
// Identification: test/sql/testing_sql_util.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "sql/testing_sql_util.h"

#include "binder/bind_node_visitor.h"
#include <random>
#include "catalog/catalog.h"
#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/plan_executor.h"
#include "gmock/gtest/gtest.h"
#include "optimizer/optimizer.h"
#include "optimizer/rule.h"
#include "parser/postgresparser.h"
#include "planner/plan_util.h"
#include "traffic_cop/tcop.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

namespace test {

std::random_device rd;
std::mt19937 rng(rd());

// Create a uniform random number
int TestingSQLUtil::GetRandomInteger(const int lower_bound,
                                     const int upper_bound) {
  std::uniform_int_distribution<> dist(lower_bound, upper_bound);

  int sample = dist(rng);
  return sample;
}

// Show the content in the specific table in the specific database
// Note: In order to see the content from the command line, you have to
// turn-on LOG_TRACE.
void TestingSQLUtil::ShowTable(std::string database_name,
                               std::string table_name) {
  ExecuteSQLQuery("SELECT * FROM " + database_name + "." + table_name);
}

// TODO(Tianyu): These testing code look copy-and-pasted. Should probably consider
// rewriting them.
// Execute a SQL query end-to-end
ResultType TestingSQLUtil::ExecuteSQLQuery(
    const std::string query, std::vector<ResultValue> &result,
    std::vector<FieldInfo> &tuple_descriptor, int &rows_changed,
    std::string &error_message) {
  LOG_TRACE("Query: %s", query.c_str());
  // prepareStatement
  std::string unnamed_statement = "unnamed";
  auto &traffic_cop = tcop::Tcop::GetInstance();
  tcop::ClientProcessState state;
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query);
  PELOTON_ASSERT(sql_stmt_list);
  if (!sql_stmt_list->is_valid) {
    return ResultType::FAILURE;
  }
  auto statement = traffic_cop.PrepareStatement(state,
                                                unnamed_statement,
                                                query,
                                                std::move(sql_stmt_list));
  if (statement.get() == nullptr) {
    state.rows_affected_ = 0;
    rows_changed = 0;
    error_message = state.error_message_;
    return ResultType::FAILURE;
  }
  // ExecuteStatment
  std::vector<type::Value> param_values;
  std::vector<PostgresDataFormat>
      result_format(statement->GetTupleDescriptor().size(),
                    PostgresDataFormat::TEXT);
  // SetTrafficCopCounter();
  counter_.store(1);
  statement.swap(state.statement_);
  state.param_values_ = param_values;
  state.result_format_ = result_format;
  state.result_ = result;
  auto status = traffic_cop.ExecuteStatement(state, [] {
    UtilTestTaskCallback(&counter_);
  });
  if (state.is_queuing_) {
    ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult(state);
    status = traffic_cop.ExecuteStatementGetResult(state);
    state.is_queuing_ = false;
  }
  if (status == ResultType::SUCCESS) {
    tuple_descriptor = state.statement_->GetTupleDescriptor();
  }
  LOG_TRACE("Statement executed. Result: %s",
            ResultTypeToString(status).c_str());
  rows_changed = state.rows_affected_;
  // TODO(Tianyu): This is a refactor in progress. This copy can be eliminated.
  result = state.result_;
  return status;
}

// Execute a SQL query end-to-end with the specific optimizer
ResultType TestingSQLUtil::ExecuteSQLQueryWithOptimizer(
    std::unique_ptr<optimizer::AbstractOptimizer> &optimizer,
    const std::string query, std::vector<ResultValue> &result,
    std::vector<FieldInfo> &tuple_descriptor, int &rows_changed,
    std::string &error_message) {
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  std::vector<type::Value> params;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto &traffic_cop = tcop::Tcop::GetInstance();
  tcop::ClientProcessState state;
  auto txn = txn_manager.BeginTransaction();
  state.tcop_txn_state_.emplace(txn, ResultType::SUCCESS);

  auto parsed_stmt = peloton_parser.BuildParseTree(query);

  auto bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
  bind_node_visitor.BindNameToNode(parsed_stmt->GetStatement(0));

  auto plan = optimizer->BuildPelotonPlanTree(parsed_stmt, txn);
  tuple_descriptor =
      traffic_cop.GenerateTupleDescriptor(state, parsed_stmt->GetStatement(0));
  auto result_format = std::vector<PostgresDataFormat>(tuple_descriptor.size(),
                                                       PostgresDataFormat::TEXT);

  try {
    LOG_TRACE("\n%s", planner::PlanUtil::GetInfo(plan.get()).c_str());
    // SetTrafficCopCounter();
    counter_.store(1);
    QueryType query_type = StatementTypeToQueryType(parsed_stmt->GetStatement(0)->GetType(),
                                                    parsed_stmt->GetStatement(0));
    state.statement_ = std::make_shared<Statement>("unnamed", query_type, query, std::move(parsed_stmt));
    state.param_values_ = params;
    state.result_ = result;
    state.result_format_ = result_format;
    auto status =
        traffic_cop.ExecuteHelper(state, [] {
          UtilTestTaskCallback(&counter_);
        });
    if (state.is_queuing_) {
      TestingSQLUtil::ContinueAfterComplete();
      traffic_cop.ExecuteStatementPlanGetResult(state);
      status = state.p_status_;
      state.is_queuing_ = false;
    }
    rows_changed = status.m_processed;
    LOG_TRACE("Statement executed. Result: %s",
              ResultTypeToString(status.m_result).c_str());
    return status.m_result;
  } catch (Exception &e) {
    error_message = e.what();
    LOG_TRACE("Testing SQL Util Caught Exception : %s", e.what());
    return ResultType::FAILURE;
  }
}

std::shared_ptr<planner::AbstractPlan>
TestingSQLUtil::GeneratePlanWithOptimizer(
    std::unique_ptr<optimizer::AbstractOptimizer> &optimizer,
    const std::string query, concurrency::TransactionContext *txn) {
  auto &peloton_parser = parser::PostgresParser::GetInstance();

  auto parsed_stmt = peloton_parser.BuildParseTree(query);

  auto bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
  bind_node_visitor.BindNameToNode(parsed_stmt->GetStatement(0));

  auto return_value = optimizer->BuildPelotonPlanTree(parsed_stmt, txn);
  return return_value;
}

ResultType TestingSQLUtil::ExecuteSQLQuery(const std::string query,
                                           std::vector<ResultValue> &result) {
  std::vector<FieldInfo> tuple_descriptor;

  // prepareStatement
  std::string unnamed_statement = "unnamed";
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query);
  PELOTON_ASSERT(sql_stmt_list);
  if (!sql_stmt_list->is_valid) {
    return ResultType::FAILURE;
  }
  auto &traffic_cop = tcop::Tcop::GetInstance();
  tcop::ClientProcessState state;
  auto statement = traffic_cop.PrepareStatement(state,
                                                unnamed_statement,
                                                query,
                                                std::move(sql_stmt_list));
  if (statement == nullptr) {
    state.rows_affected_ = 0;
    return ResultType::FAILURE;
  }
  // ExecuteStatment
  std::vector<type::Value> param_values;
  std::vector<PostgresDataFormat>
      result_format(statement->GetTupleDescriptor().size(),
                    PostgresDataFormat::TEXT);
  // SetTrafficCopCounter();
  counter_.store(1);
  state.statement_.reset(statement.get());
  state.param_values_ = param_values;
  state.result_format_ = result_format;
  state.result_ = result;
  auto status = traffic_cop.ExecuteStatement(state, [] {
    UtilTestTaskCallback(&counter_);
  });
  if (state.is_queuing_) {
    ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult(state);
    status = traffic_cop.ExecuteStatementGetResult(state);
    state.is_queuing_ = false;
  }
  if (status == ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }
  // TODO(Tianyu) Same as above.
  result = state.result_;
  return status;
}

ResultType TestingSQLUtil::ExecuteSQLQuery(const std::string query) {
  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;

  // execute the query using tcop
  // prepareStatement
  LOG_TRACE("Query: %s", query.c_str());
  std::string unnamed_statement = "unnamed";
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query);
  PELOTON_ASSERT(sql_stmt_list);
  if (!sql_stmt_list->is_valid) {
    return ResultType::FAILURE;
  }
  auto &traffic_cop = tcop::Tcop::GetInstance();
  tcop::ClientProcessState state;
  auto statement = traffic_cop.PrepareStatement(state,
                                                unnamed_statement,
                                                query,
                                                std::move(sql_stmt_list));
  if (statement == nullptr) {
    state.rows_affected_ = 0;
    return ResultType::FAILURE;
  }
  // ExecuteStatement
  std::vector<type::Value> param_values;
  std::vector<PostgresDataFormat> result_format(statement->GetTupleDescriptor().size(),
                                                 PostgresDataFormat::TEXT);
  // SetTrafficCopCounter();
  counter_.store(1);
  statement.swap(state.statement_);
  state.param_values_ = param_values;
  state.result_format_ = result_format;
  state.result_ = result;
  auto status = traffic_cop.ExecuteStatement(state, []{
    UtilTestTaskCallback(&counter_);
  });
  if (state.is_queuing_) {
    ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult(state);
    status = traffic_cop.ExecuteStatementGetResult(state);
    state.is_queuing_ = false;
  }
  if (status == ResultType::SUCCESS) {
    tuple_descriptor = state.statement_->GetTupleDescriptor();
  }
  LOG_TRACE("Statement executed. Result: %s",
            ResultTypeToString(status).c_str());
  return status;
}

// Executes a query and compares the result with the given rows, either
// ordered or not
// The result vector has to be specified as follows:
// {"1|string1", "2|strin2", "3|string3"}
void TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
    std::string query, std::vector<std::string> ref_result, bool ordered) {
  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  // Execute query
  TestingSQLUtil::ExecuteSQLQuery(std::move(query), result, tuple_descriptor,
                                  rows_changed, error_message);
  if (tuple_descriptor.size() == 0) {
    PELOTON_ASSERT(result.size() == 0);
    return;
  }
  unsigned int rows = result.size() / tuple_descriptor.size();

  // Build actual result as set of rows for comparison
  std::vector<std::string> actual_result(rows);
  for (unsigned int i = 0; i < rows; i++) {
    std::string row_string;

    for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
      row_string +=
          GetResultValueAsString(result, i * tuple_descriptor.size() + j);
      if (j < tuple_descriptor.size() - 1) {
        row_string += "|";
      }
    }

    actual_result[i] = std::move(row_string);
  }

  // Compare actual result to expected result
  EXPECT_EQ(ref_result.size(), actual_result.size());

  for (auto &actual_row : actual_result) {
    if (ordered) {
      // ordered: check if the row at the same index is equal
      auto index = &actual_row - &actual_result[0];

      if (actual_row != ref_result[index]) {
        EXPECT_EQ(ref_result, actual_result);
        break;
      }
    } else {
      // unordered: find this row and remove it
      std::vector<std::string>::iterator position =
          std::find(ref_result.begin(), ref_result.end(), actual_row);

      if (position != ref_result.end()) {
        ref_result.erase(position);
      } else {
        EXPECT_EQ(ref_result, actual_result);
        break;
      }
    }
  }
}

void TestingSQLUtil::ContinueAfterComplete() {
  while (TestingSQLUtil::counter_.load() == 1) {
    usleep(10);
  }
}

void TestingSQLUtil::UtilTestTaskCallback(void *arg) {
  std::atomic_int *count = static_cast<std::atomic_int *>(arg);
  count->store(0);
}

std::atomic_int TestingSQLUtil::counter_;

}  // namespace test
}  // namespace peloton
