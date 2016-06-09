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

#include "parser/parser/pg_query.h"

namespace peloton {
namespace tcop {

// global singleton
TrafficCop &TrafficCop::GetInstance(void) {
  static TrafficCop traffic_cop;
  return traffic_cop;
}

TrafficCop::TrafficCop() {

  // Initialize the postgresult memory context
  pg_query_init();

}

TrafficCop::~TrafficCop() {

  // Destroy the postgresult memory context
  pg_query_destroy();

}

Result TrafficCop::ExecuteStatement(const std::string& query,
                                    std::vector<ResultType> &result,
                                    std::vector<FieldInfoType> &tuple_descriptor,
                                    int &rows_changed,
                                    std::string &error_message){
  LOG_INFO("Received %s", query.c_str());

  // Prepare the statement
  auto statement = PrepareStatement(query, error_message);

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

  return Result::RESULT_FAILURE;
}

std::shared_ptr<Statement> TrafficCop::PrepareStatement(UNUSED_ATTRIBUTE const std::string& query,
                                                        UNUSED_ATTRIBUTE std::string &error_message){
  std::shared_ptr<Statement> statement;

  LOG_INFO("Prepare Statement %s", query.c_str());

  // This will construct a plan tree
  // And set the tuple descriptor

  return statement;
}


}  // End tcop namespace
}  // End peloton namespace
