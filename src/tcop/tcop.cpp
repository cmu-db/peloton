//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_manager.cpp
//
// Identification: src/common/thread_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "tcop/tcop.h"

#include "common/macros.h"
#include "common/portal.h"
#include "common/logger.h"
#include "common/types.h"

#include "parser/postgres_parser.h"
#include "optimizer/simple_optimizer.h"

namespace peloton {
namespace tcop {

// global singleton
TrafficCop &TrafficCop::GetInstance(void) {
  static TrafficCop traffic_cop;
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
    tuple_descriptor = std::move(statement->GetTupleDescriptor());
  }

  return status;
}

Result TrafficCop::ExecuteStatement(UNUSED_ATTRIBUTE const std::shared_ptr<Statement>& statement,
                                    UNUSED_ATTRIBUTE const bool unnamed,
                                    UNUSED_ATTRIBUTE std::vector<ResultType> &result,
                                    UNUSED_ATTRIBUTE int &rows_changed,
                                    UNUSED_ATTRIBUTE std::string &error_message){

  LOG_INFO("Execute Statement %s", statement->GetStatementName().c_str());

  // This will construct an executor tree
  // And set the tuple descriptor

  return Result::RESULT_FAILURE;
}

std::shared_ptr<Statement> TrafficCop::PrepareStatement(const std::string& statement_name,
                                                        const std::string& query_string,
                                                        UNUSED_ATTRIBUTE std::string &error_message){
  std::shared_ptr<Statement> statement;

  LOG_INFO("Prepare Statement %s", query_string.c_str());
<<<<<<< HEAD

  statement.reset(new Statement(statement_name, query_string));
=======
>>>>>>> 82049dc46b6d65a69bf79b21d38a2786574f76aa

  statement.reset(new Statement(statement_name, query_string));

  auto& postgres_parser = parser::PostgresParser::GetInstance();
  auto parse_tree = postgres_parser.BuildParseTree(query_string);

  statement->SetPlanTree(optimizer::SimpleOptimizer::BuildPlanTree(parse_tree));

  return statement;
}


}  // End tcop namespace
}  // End peloton namespace
