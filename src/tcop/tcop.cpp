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

#include "common/abstract_tuple.h"
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/portal.h"
#include "common/type.h"
#include "common/types.h"

#include "expression/parser_expression.h"

#include "parser/parser.h"

#include "catalog/catalog.h"
#include "executor/plan_executor.h"
#include "optimizer/simple_optimizer.h"

#include <boost/algorithm/string.hpp>

namespace peloton {
namespace tcop {

// global singleton
TrafficCop &TrafficCop::GetInstance(void) {
  static TrafficCop traffic_cop;
  return traffic_cop;
}

TrafficCop::TrafficCop() {
  // Nothing to do here !
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);
}

TrafficCop::~TrafficCop() {
  // Nothing to do here !
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
    const std::vector<common::Value> &params,
    UNUSED_ATTRIBUTE const bool unnamed,
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
  try {
    std::vector<common::Value> params;
    bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
    bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(
        statement->GetPlanTree().get(), params, result, result_format);
    LOG_TRACE("Statement executed. Result: %d", status.m_result);
    rows_changed = status.m_processed;
    return status.m_result;
  }
  catch (Exception &e) {
    error_message = e.what();
    return Result::RESULT_FAILURE;
  }
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
    statement->SetPlanTree(
        optimizer::SimpleOptimizer::BuildPelotonPlanTree(sql_stmt));

    for (auto stmt : sql_stmt->GetStatements()) {
      if (stmt->GetType() == STATEMENT_TYPE_SELECT) {
        auto tuple_descriptor = GenerateTupleDescriptor(query_string);
        statement->SetTupleDescriptor(tuple_descriptor);
      }
      break;
    }

    bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
    LOG_DEBUG("Statement Prepared!");
    return std::move(statement);
  }
  catch (Exception &e) {
    error_message = e.what();
    return nullptr;
  }
}

std::vector<FieldInfoType> TrafficCop::GenerateTupleDescriptor(
    std::string query) {
  std::vector<FieldInfoType> tuple_descriptor;

  // Set up parser
  auto &peloton_parser = parser::Parser::GetInstance();
  auto sql_stmt = peloton_parser.BuildParseTree(query);

  auto first_stmt = sql_stmt->GetStatement(0);

  if (first_stmt->GetType() != STATEMENT_TYPE_SELECT) return tuple_descriptor;

  // Get the Select Statement
  auto select_stmt = (parser::SelectStatement *)first_stmt;

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

  // Get the columns information and set up
  // the columns description for the returned results
  for (auto expr : *select_stmt->select_list) {
    if (expr->GetExpressionType() == EXPRESSION_TYPE_STAR) {
      for (auto column : table_columns) {
        tuple_descriptor.push_back(
            GetColumnFieldForValueType(column.column_name, column.column_type));
      }
    }

    // if query has only certain columns
    if (expr->GetExpressionType() == EXPRESSION_TYPE_COLUMN_REF) {
      // Get the column name
      auto col_name = expr->GetName();

      // Traverse the table's columns
      for (auto column : table_columns) {
        // check if the column name matches
        if (column.column_name == col_name) {
          tuple_descriptor.push_back(
              GetColumnFieldForValueType(col_name, column.column_type));
        }
      }
    }

    // Query has aggregation Functions
    if (expr->GetExpressionType() == EXPRESSION_TYPE_FUNCTION_REF) {
      // Get the parser expression that contains
      // the typr of the aggreataion function
      auto func_expr = (expression::ParserExpression *)expr;

      std::string col_name = "";
      // check if expression has an alias
      if (expr->alias != NULL) {
        tuple_descriptor.push_back(GetColumnFieldForAggregates(
            std::string(expr->alias), func_expr->expr->GetExpressionType()));
      } else {
        // Construct a String
        std::string agg_func_name = std::string(expr->GetName());
        std::string col_name = std::string(func_expr->expr->GetName());

        // Example : Count(id)
        std::string full_agg_name = agg_func_name + "(" + col_name + ")";

        tuple_descriptor.push_back(GetColumnFieldForAggregates(
            full_agg_name, func_expr->expr->GetExpressionType()));
      }
    }
  }
  return tuple_descriptor;
}

FieldInfoType TrafficCop::GetColumnFieldForValueType(
    std::string column_name, common::Type::TypeId column_type) {
  switch (column_type) {
    case common::Type::INTEGER:
      return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_INTEGER, 4);
    case common::Type::DECIMAL:
      return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_DOUBLE, 8);
    case common::Type::VARCHAR:
    case common::Type::VARBINARY:
      return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_TEXT, 255);
    case common::Type::TIMESTAMP:
      return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_TIMESTAMPS, 64);
    default:
      // Type not Identified
      LOG_ERROR("Unrecognized column type: %d", column_type);
      // return String
      return std::make_tuple(column_name, POSTGRES_VALUE_TYPE_TEXT, 255);
  }
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
