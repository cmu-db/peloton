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
    int &rows_changed, UNUSED_ATTRIBUTE std::string &error_message) {

  LOG_TRACE("Execute Statement %s", statement->GetStatementName().c_str());
  std::vector<Value> params;
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(
      statement->GetPlanTree().get(), params, result);
  LOG_TRACE("Statement executed. Result: %d", status.m_result);

  rows_changed = status.m_processed;
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

  for(auto stmt : sql_stmt->GetStatements()){
    if(stmt->GetType() == STATEMENT_TYPE_SELECT){
      auto tuple_descriptor = GenerateTupleDescriptor(query_string);
      statement->SetTupleDescriptor(tuple_descriptor);
    }
    break;
  }

  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  LOG_TRACE("Statement Prepared!");
  return std::move(statement);
}

std::vector<FieldInfoType> TrafficCop::GenerateTupleDescriptor(std::string query) {

  std::vector<FieldInfoType> tuple_descriptor;

  // Set up parser
  auto &peloton_parser = parser::Parser::GetInstance();
  auto sql_stmt = peloton_parser.BuildParseTree(query);

  auto first_stmt = sql_stmt->GetStatement(0);

  //Get the Select Statement
  auto select_stmt = (parser::SelectStatement *)first_stmt;

  // Set up the table
  storage::DataTable *target_table = nullptr;

  // Check if query only has one Table
  // Example : SELECT * FROM A;
  if (select_stmt->from_table->list == NULL){
    target_table = static_cast<storage::DataTable *>(
              catalog::Bootstrapper::global_catalog->GetTableFromDatabase(
                  DEFAULT_DB_NAME, select_stmt->from_table->name));
  }

  // Query has multiple tables
  // Example: SELECT COUNT(ID) FROM A,B <Condition>
  // For now we only pick the first table in the list
  // FIX: Better handle for queries with multiple tables
  else{
    for(auto table : *select_stmt->from_table->list){
      target_table = static_cast<storage::DataTable *>(
                catalog::Bootstrapper::global_catalog->GetTableFromDatabase(
                    DEFAULT_DB_NAME, table->name));
      break;
    }

  }

  // Get the columns of the table
  auto table_columns = target_table->GetSchema()->GetColumns();

  // Get the columns information and set up
  // the columns description for the returned results
  for (auto expr : *select_stmt->select_list){
    if(expr->GetExpressionType() == EXPRESSION_TYPE_STAR){
      for(auto column : table_columns){
        tuple_descriptor.push_back(GetColumnFieldForValueType(column.column_name,column.column_type));
      }
    }

    //if query has only certain columns
    if(expr->GetExpressionType() == EXPRESSION_TYPE_COLUMN_REF){

      // Get the column name
      auto col_name = expr->GetName();

      // Traverse the table's columns
      for(auto column : table_columns){
        // check if the column name matches
        if(column.column_name == col_name){
          tuple_descriptor.push_back(GetColumnFieldForValueType(col_name , column.column_type));
        }
      }
    }

    // Query has aggregation Functions
    if(expr->GetExpressionType() == EXPRESSION_TYPE_FUNCTION_REF){
      //Get the parser expression that contains
      //the typr of the aggreataion function
      auto func_expr = (expression::ParserExpression*)expr;

      std::string col_name = "";
      //check if expression has an alias
      if(expr->alias != NULL){
        tuple_descriptor.push_back(GetColumnFieldForAggregates(std::string(expr->alias) , func_expr->expr->GetExpressionType()));
      }
      else{
        //Construct a String
        std::string agg_func_name = std::string(expr->GetName());
        std::string col_name = std::string(func_expr->expr->GetName());

        // Example : Count(id)
        std::string full_agg_name = agg_func_name + "(" + col_name + ")";

        tuple_descriptor.push_back(GetColumnFieldForAggregates(full_agg_name , func_expr->expr->GetExpressionType()));
      }

    }
  }
  return tuple_descriptor;
}

FieldInfoType TrafficCop::GetColumnFieldForValueType(std::string column_name , ValueType column_type){
  if(column_type == VALUE_TYPE_INTEGER){
    return std::make_tuple(column_name , POSTGRES_VALUE_TYPE_INTEGER , 4);
  }

  if(column_type == VALUE_TYPE_DOUBLE){
    return std::make_tuple(column_name , POSTGRES_VALUE_TYPE_DOUBLE , 8);
  }

  if(column_type == VALUE_TYPE_VARCHAR){
    return std::make_tuple(column_name , POSTGRES_VALUE_TYPE_TEXT , 255);
  }

  if(column_type == VALUE_TYPE_DECIMAL){
    return std::make_tuple(column_name , POSTGRES_VALUE_TYPE_DECIMAL , 16);
  }

  if(column_type == VALUE_TYPE_TIMESTAMP){
    return std::make_tuple(column_name , POSTGRES_VALUE_TYPE_TIMESTAMPS , 64);
  }

  // Type not Identified
  LOG_ERROR("Unrecognized column type: %d", column_type);
  //return String
  return std::make_tuple(column_name , POSTGRES_VALUE_TYPE_TEXT , 255);
}

FieldInfoType TrafficCop::GetColumnFieldForAggregates(std::string name , ExpressionType expr_type){

  //For now we only return INT for (MAX , MIN)
  //TODO: Check if column type is DOUBLE and return it for (MAX. MIN)

  // Check the expression type and return the corresponding description
  if(expr_type == EXPRESSION_TYPE_AGGREGATE_MAX || expr_type == EXPRESSION_TYPE_AGGREGATE_MIN || expr_type == EXPRESSION_TYPE_AGGREGATE_COUNT){
    return std::make_tuple(name ,POSTGRES_VALUE_TYPE_INTEGER, 4);
  }

  // Return double if function is AVERAGE
  if(expr_type == EXPRESSION_TYPE_AGGREGATE_AVG){
    return std::make_tuple(name , POSTGRES_VALUE_TYPE_DOUBLE, 8);
  }

  if(expr_type == EXPRESSION_TYPE_AGGREGATE_COUNT_STAR){
    return std::make_tuple("COUNT(*)", POSTGRES_VALUE_TYPE_INTEGER, 4);
  }

  return std::make_tuple(name , POSTGRES_VALUE_TYPE_TEXT , 255);
}
}  // End tcop namespace
}  // End peloton namespace
