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

#include "parser/parser/pg_query.h"

namespace peloton {
namespace tcop {

// global singleton
TrafficCop &TrafficCop::GetInstance(void) {
  static TrafficCop traffic_cop;
  return traffic_cop;
}

TrafficCop::TrafficCop() {

  // Initialize the postgres memory context
  pg_query_init();

}

TrafficCop::~TrafficCop() {

  // Destroy the postgres memory context
  pg_query_destroy();

}

int TrafficCop::PortalExec(UNUSED_ATTRIBUTE const char *query,
                           UNUSED_ATTRIBUTE std::vector<ResType> &res,
                           UNUSED_ATTRIBUTE std::vector<FieldInfoType> &info,
                           UNUSED_ATTRIBUTE int &rows_change,
                           UNUSED_ATTRIBUTE std::string &err_msg){
  return 0;
}

int TrafficCop::PrepareStmt(UNUSED_ATTRIBUTE const char *query,
                            UNUSED_ATTRIBUTE PreparedStatement **stmt,
                            UNUSED_ATTRIBUTE std::string &err_msg){
  return 0;
}

int TrafficCop::BindStmt(UNUSED_ATTRIBUTE std::vector<std::pair<int, std::string>> &parameters,
                         UNUSED_ATTRIBUTE PreparedStatement **stmt,
                         UNUSED_ATTRIBUTE std::string &err_msg){
  return 0;
}

void TrafficCop::GetRowDesc(UNUSED_ATTRIBUTE void *stmt,
                            UNUSED_ATTRIBUTE std::vector<FieldInfoType> &info){
}

int TrafficCop::ExecPrepStmt(UNUSED_ATTRIBUTE void *stmt,
                             UNUSED_ATTRIBUTE bool unnamed,
                             UNUSED_ATTRIBUTE std::vector<ResType> &res,
                             UNUSED_ATTRIBUTE int &rows_change,
                             UNUSED_ATTRIBUTE std::string &err_msg){
  return 0;
}

void TrafficCop::CopyFromTo(UNUSED_ATTRIBUTE const char *src,
                            UNUSED_ATTRIBUTE std::vector<unsigned char> &dst){
}

int TrafficCop::ExecCallback(UNUSED_ATTRIBUTE void *res,
                             UNUSED_ATTRIBUTE int argc,
                             UNUSED_ATTRIBUTE char **argv,
                             UNUSED_ATTRIBUTE char **azColName){
  return 0;
}

}  // End tcop namespace
}  // End peloton namespace
