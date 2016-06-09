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

Result TrafficCop::ExecuteStatement(UNUSED_ATTRIBUTE const std::string& query,
                                    UNUSED_ATTRIBUTE std::vector<ResultType> &result,
                                    UNUSED_ATTRIBUTE std::vector<FieldInfoType> &info,
                                    UNUSED_ATTRIBUTE int &rows_changed,
                                    UNUSED_ATTRIBUTE std::string &error_message){
  return Result::RESULT_FAILURE;
}


Result TrafficCop::ExecutePreparedStatement(UNUSED_ATTRIBUTE const std::shared_ptr<PreparedStatement>& prepared_statement,
                                            UNUSED_ATTRIBUTE bool unnamed,
                                            UNUSED_ATTRIBUTE std::vector<ResultType> &result,
                                            UNUSED_ATTRIBUTE int &rows_change,
                                            UNUSED_ATTRIBUTE std::string &error_message){

  return Result::RESULT_FAILURE;
}

std::shared_ptr<PreparedStatement> TrafficCop::PrepareStatement(UNUSED_ATTRIBUTE const std::string& query,
                                                                UNUSED_ATTRIBUTE std::string &error_message){
  std::shared_ptr<PreparedStatement> prepared_statement;

  return prepared_statement;
}


}  // End tcop namespace
}  // End peloton namespace
