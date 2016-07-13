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

#include "parser/parser.h"

#include "optimizer/simple_optimizer.h"
#include "executor/plan_executor.h"
#include "catalog/bootstrapper.h"


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

Result TrafficCop::ExecuteStatement(const std::string& query,
                                    std::vector<ResultType> &result,
                                    std::vector<FieldInfoType> &tuple_descriptor,
                                    int &rows_changed,
                                    std::string &error_message){
  LOG_INFO("Received %s", query.c_str());

  // Prepare the statement
  std::string unnamed_statement = "unnamed";
  auto statement = PrepareStatement(unnamed_statement, query, error_message);

  if(statement.get() == nullptr){
    return Result::RESULT_FAILURE;
  }

  // Then, execute the statement
  bool unnamed = true;
  auto status = ExecuteStatement(statement, unnamed,
                                 result, rows_changed, error_message);

  if(status == Result::RESULT_SUCCESS) {
    LOG_INFO("Execution succeeded!");
    tuple_descriptor = std::move(statement->GetTupleDescriptor());
  }
  else{
    LOG_INFO("Execution failed!");
  }

  return status;
}

Result TrafficCop::ExecuteStatement(UNUSED_ATTRIBUTE const std::shared_ptr<Statement>& statement,
                                    UNUSED_ATTRIBUTE const bool unnamed,
                                    UNUSED_ATTRIBUTE std::vector<ResultType> &result,
                                    UNUSED_ATTRIBUTE int &rows_changed,
                                    UNUSED_ATTRIBUTE std::string &error_message){

  LOG_INFO("Execute Statement %s", statement->GetStatementName().c_str());
  std::vector<Value> params;
  bridge::PlanExecutor::PrintPlan(statement->GetPlanTree().get(), "Plan");
  bridge::peloton_status status = bridge::PlanExecutor::ExecutePlan(statement->GetPlanTree().get(), params);
  LOG_INFO("Statement executed. Result: %d", status.m_result);
  return status.m_result;

}

std::shared_ptr<Statement> TrafficCop::PrepareStatement(const std::string& statement_name,
                                                        const std::string& query_string,
                                                        UNUSED_ATTRIBUTE std::string &error_message){
  std::shared_ptr<Statement> statement;

  LOG_INFO("Prepare Statement %s", query_string.c_str());

  statement.reset(new Statement(statement_name, query_string));

  auto& peloton_parser = parser::Parser::GetInstance();
  auto sql_stmt = peloton_parser.BuildParseTree(query_string);

  statement->SetPlanTree(optimizer::SimpleOptimizer::BuildPelotonPlanTree(sql_stmt));

  LOG_INFO("Statement Prepared!");
  return statement;

}


}  // End tcop namespace
} // End peloton namespace
