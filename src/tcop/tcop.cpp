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

#include "common/macros.h"
#include "common/portal.h"
#include "common/logger.h"
#include "common/types.h"
#include "common/abstract_tuple.h"

#include "expression/parser_expression.h"

#include "parser/parser.h"

#include "optimizer/simple_optimizer.h"
#include "executor/plan_executor.h"
#include "catalog/bootstrapper.h"

#include <boost/algorithm/string.hpp>

namespace peloton {
namespace tcop {

// global singleton
TrafficCop &TrafficCop::GetInstance(void) {
  static TrafficCop traffic_cop;
  catalog::Bootstrapper::bootstrap();
  catalog::Bootstrapper::global_catalog->CreateDatabase(DEFAULT_DB_NAME);
  return traffic_cop;
}

TrafficCop::TrafficCop() {
  // Nothing to do here !
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
  auto status =
      ExecuteStatement(statement, unnamed, result, rows_changed, error_message);

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
    UNUSED_ATTRIBUTE const bool unnamed, std::vector<ResultType> &result,
    UNUSED_ATTRIBUTE int &rows_changed,
    UNUSED_ATTRIBUTE std::string &error_message) {

  LOG_TRACE("Execute Statement %s", statement->GetStatementName().c_str());
  std::vector<Value> params;
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(
      statement->GetPlanTree().get(), params, result);
  LOG_TRACE("Statement executed. Result: %d", status.m_result);
  return status.m_result;
}

std::shared_ptr<Statement> TrafficCop::PrepareStatement(
    const std::string &statement_name, const std::string &query_string,
    UNUSED_ATTRIBUTE std::string &error_message) {
  std::shared_ptr<Statement> statement;

  LOG_TRACE("Prepare Statement %s", query_string.c_str());

  statement.reset(new Statement(statement_name, query_string));

  auto &peloton_parser = parser::Parser::GetInstance();
  auto sql_stmt = peloton_parser.BuildParseTree(query_string);

  statement->SetPlanTree(
      optimizer::SimpleOptimizer::BuildPelotonPlanTree(sql_stmt));

  auto t_desc = GenerateTupleDescriptor(query_string);
  
  statement->SetTupleDescriptor(t_desc);

  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_TRACE("Statement Prepared!");
  return std::move(statement);
}

std::vector<FieldInfoType> TrafficCop::GenerateTupleDescriptor(std::string query){

  // The tuple descriptor to be returned
  std::vector<FieldInfoType> t_desc;
  
  std::vector<std::string> query_tokens;
  boost::split(query_tokens, query, boost::is_any_of(" "),
               boost::token_compress_on);

  //Check if Select
  if(query_tokens[0] == "SELECT"){

    auto &peloton_parser = parser::Parser::GetInstance();
    auto sql_stmt = peloton_parser.BuildParseTree(query);

    auto select_stmt = (parser::SelectStatement *) sql_stmt.get();

    auto target_table = static_cast<storage::DataTable *>(
          catalog::Bootstrapper::global_catalog->GetTableFromDatabase(
              DEFAULT_DB_NAME, select_stmt->from_table->name));

    auto columns = target_table->GetSchema()->GetColumns();

    for(auto expr : *select_stmt->select_list){
      if(expr->GetExpressionType() == EXPRESSION_TYPE_STAR){

        for (oid_t i = 0; i < columns.size(); ++i) {

          if (columns[i].column_type == VALUE_TYPE_INTEGER) {
            t_desc.push_back(std::make_tuple(columns[i].column_name, 23, 4));
          } else if (columns[i].column_type == VALUE_TYPE_DOUBLE) {
            t_desc.push_back(std::make_tuple(columns[i].column_name, 701, 8));
          } else if (columns[i].column_type == VALUE_TYPE_VARCHAR) {
            t_desc.push_back(std::make_tuple(columns[i].column_name, 25, 255));
          } else if (columns[i].column_type == VALUE_TYPE_DECIMAL) {
            t_desc.push_back(std::make_tuple(columns[i].column_name, 1700, 16));
          } else {
            LOG_ERROR("Unrecognized column type: %d", columns[i].column_type);
            t_desc.push_back(std::make_tuple(columns[i].column_name, 25, 255));
          }
        }

      }
      else if (expr->GetExpressionType() == EXPRESSION_TYPE_COLUMN_REF){
        auto col_name = expr->GetName();
        for(oid_t i = 0; i < columns.size(); ++i){
          if(col_name == columns[i].column_name){
            if (columns[i].column_type == VALUE_TYPE_INTEGER) {
            t_desc.push_back(std::make_tuple(columns[i].column_name, 23, 4));
          } else if (columns[i].column_type == VALUE_TYPE_DOUBLE) {
            t_desc.push_back(std::make_tuple(columns[i].column_name, 701, 8));
          } else if (columns[i].column_type == VALUE_TYPE_VARCHAR) {
            t_desc.push_back(std::make_tuple(columns[i].column_name, 25, 255));
          } else if (columns[i].column_type == VALUE_TYPE_DECIMAL) {
            t_desc.push_back(std::make_tuple(columns[i].column_name, 1700, 16));
          } else {
            LOG_ERROR("Unrecognized column type: %d", columns[i].column_type);
            t_desc.push_back(std::make_tuple(columns[i].column_name, 25, 255));
          }
          }
        }
      }
      else if(expr->GetExpressionType() == EXPRESSION_TYPE_FUNCTION_REF){
        auto func_expr = (expression::ParserExpression *) expr;
        if(func_expr->expr->GetExpressionType() == EXPRESSION_TYPE_COLUMN_REF){
          std::string col_name = std::string(func_expr->GetName()) + "(" + std::string(func_expr->expr->GetName()) + ")";
          t_desc.push_back(std::make_tuple(col_name, 23, 4));
        }
        else if(func_expr->expr->GetExpressionType() == EXPRESSION_TYPE_AGGREGATE_AVG){
          std::string col_name = std::string(func_expr->GetName()) + "(" + std::string(func_expr->expr->GetName()) + ")";
          t_desc.push_back(std::make_tuple(col_name, 701, 8)); 
        }
        else if(func_expr->expr->GetExpressionType() == EXPRESSION_TYPE_AGGREGATE_MAX){
          std::string col_name = std::string(func_expr->GetName()) + "(" + std::string(func_expr->expr->GetName()) + ")";
          t_desc.push_back(std::make_tuple(col_name, 25, 4)); 
        }
        else if(func_expr->expr->GetExpressionType() == EXPRESSION_TYPE_AGGREGATE_MIN){
          std::string col_name = std::string(func_expr->GetName()) + "(" + std::string(func_expr->expr->GetName()) + ")";
          t_desc.push_back(std::make_tuple(col_name, 25, 4)); 
        }
        else if(func_expr->expr->GetExpressionType() == EXPRESSION_TYPE_AGGREGATE_COUNT_STAR){
          std::string col_name = "COUNT(*)";
          t_desc.push_back(std::make_tuple(col_name, 701, 8)); 
        }
      } 
    }
  }

  return t_desc;
}

}  // End tcop namespace
}  // End peloton namespace
