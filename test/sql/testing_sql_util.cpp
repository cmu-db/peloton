//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_tests_util.cpp
//
// Identification: test/sql/sql_tests_util.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "sql/testing_sql_util.h"
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
#include "traffic_cop/traffic_cop.h"

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

// Execute a SQL query end-to-end
ResultType TestingSQLUtil::ExecuteSQLQuery(
    const std::string query, std::vector<ResultValue> &result,
    std::vector<FieldInfo> &tuple_descriptor, int &rows_changed,
    std::string &error_message) {
  LOG_INFO("Query: %s", query.c_str());
  // prepareStatement
  std::string unnamed_statement = "unnamed";
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query);
  PL_ASSERT(sql_stmt_list);
  if (!sql_stmt_list -> is_valid) {
    return ResultType::FAILURE;
  }
  auto statement = traffic_cop_.PrepareStatement(unnamed_statement, query, std::move(sql_stmt_list),
                                                 error_message);
  if (statement.get() == nullptr) {
    traffic_cop_.setRowsAffected(0);
    rows_changed = 0;
    return ResultType::FAILURE;
  }
  // ExecuteStatment
  std::vector<type::Value> param_values;
  bool unnamed = false;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  // SetTrafficCopCounter();
  counter_.store(1);
  auto status =
      traffic_cop_.ExecuteStatement(statement, param_values, unnamed, nullptr,
                                    result_format, result, error_message);
  if (traffic_cop_.GetQueuing()) {
    ContinueAfterComplete();
    traffic_cop_.ExecuteStatementPlanGetResult();
    status = traffic_cop_.ExecuteStatementGetResult();
    traffic_cop_.SetQueuing(false);
  }
  if (status == ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status).c_str());
  rows_changed = traffic_cop_.getRowsAffected();
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
  auto txn = txn_manager.BeginTransaction();
  traffic_cop_.SetTcopTxnState(txn);

  auto parsed_stmt = peloton_parser.BuildParseTree(query);
  auto plan =
      optimizer->BuildPelotonPlanTree(parsed_stmt, DEFAULT_DB_NAME, txn);
  tuple_descriptor =
      traffic_cop_.GenerateTupleDescriptor(parsed_stmt->GetStatement(0));
  auto result_format = std::vector<int>(tuple_descriptor.size(), 0);

  try {
    LOG_DEBUG("\n%s", planner::PlanUtil::GetInfo(plan.get()).c_str());
    // SetTrafficCopCounter();
    counter_.store(1);
    auto status =
        traffic_cop_.ExecuteHelper(plan, params, result, result_format);
    if (traffic_cop_.GetQueuing()) {
      TestingSQLUtil::ContinueAfterComplete();
      traffic_cop_.ExecuteStatementPlanGetResult();
      status = traffic_cop_.p_status_;
      traffic_cop_.SetQueuing(false);
    }
    rows_changed = status.m_processed;
    LOG_INFO("Statement executed. Result: %s",
             ResultTypeToString(status.m_result).c_str());
    return status.m_result;
  } catch (Exception &e) {
    error_message = e.what();
    return ResultType::FAILURE;
  }
}

std::shared_ptr<planner::AbstractPlan>
TestingSQLUtil::GeneratePlanWithOptimizer(
    std::unique_ptr<optimizer::AbstractOptimizer> &optimizer,
    const std::string query, concurrency::TransactionContext *txn) {
  auto &peloton_parser = parser::PostgresParser::GetInstance();

  auto parsed_stmt = peloton_parser.BuildParseTree(query);
  auto return_value =
      optimizer->BuildPelotonPlanTree(parsed_stmt, DEFAULT_DB_NAME, txn);
  return return_value;
}

ResultType TestingSQLUtil::ExecuteSQLQuery(const std::string query,
                                           std::vector<ResultValue> &result) {
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;

  // prepareStatement
  std::string unnamed_statement = "unnamed";
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query);
  PL_ASSERT(sql_stmt_list);
  if (!sql_stmt_list -> is_valid) {
    return ResultType::FAILURE;
  }
  auto statement = traffic_cop_.PrepareStatement(unnamed_statement, query, std::move(sql_stmt_list),
                                                 error_message);
  if (statement.get() == nullptr) {
    traffic_cop_.setRowsAffected(0);
    return ResultType::FAILURE;
  }
  // ExecuteStatment
  std::vector<type::Value> param_values;
  bool unnamed = false;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  // SetTrafficCopCounter();
  counter_.store(1);
  auto status =
      traffic_cop_.ExecuteStatement(statement, param_values, unnamed, nullptr,
                                    result_format, result, error_message);
  if (traffic_cop_.GetQueuing()) {
    ContinueAfterComplete();
    traffic_cop_.ExecuteStatementPlanGetResult();
    status = traffic_cop_.ExecuteStatementGetResult();
    traffic_cop_.SetQueuing(false);
  }
  if (status == ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }
  return status;
}

ResultType TestingSQLUtil::ExecuteSQLQuery(const std::string query) {
  std::vector<ResultValue> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;

  // execute the query using tcop
  // prepareStatement
  std::string unnamed_statement = "unnamed";
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query);
  PL_ASSERT(sql_stmt_list);
  if (!sql_stmt_list -> is_valid) {
    return ResultType::FAILURE;
  }
  auto statement = traffic_cop_.PrepareStatement(unnamed_statement, query, std::move(sql_stmt_list),
                                                 error_message);
  if (statement.get() == nullptr) {
    traffic_cop_.setRowsAffected(0);
    return ResultType::FAILURE;
  }
  // ExecuteStatment
  std::vector<type::Value> param_values;
  bool unnamed = false;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  // SetTrafficCopCounter();
  counter_.store(1);
  auto status =
      traffic_cop_.ExecuteStatement(statement, param_values, unnamed, nullptr,
                                    result_format, result, error_message);
  if (traffic_cop_.GetQueuing()) {
    ContinueAfterComplete();
    traffic_cop_.ExecuteStatementPlanGetResult();
    status = traffic_cop_.ExecuteStatementGetResult();
    traffic_cop_.SetQueuing(false);
  }
  if (status == ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }
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
tcop::TrafficCop TestingSQLUtil::traffic_cop_(UtilTestTaskCallback, &counter_);

}  // namespace test
}  // namespace peloton
