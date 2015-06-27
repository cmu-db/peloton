/*-------------------------------------------------------------------------
 *
 * scheduler_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/tests/scheduler/scheduler_test.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gtest/gtest.h"

#include "harness.h"
#include "backend/scheduler/traffic_cop.h"
#include "backend/scheduler/scheduler.h"
#include "backend/main/kernel.h"
#include "backend/catalog/catalog.h"

#include <sstream>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Scheduler Tests
//===--------------------------------------------------------------------===//

TEST(SchedulerTests, TrafficCopTest) {

  std::istringstream stream(
      "1 0 process txn 1 query 1 \n"
      "1 100 process txn 1 query 2 \n"
      "1 0 process txn 2 query 1 \n"
      "1 100 process txn 1 query 3 \n"
      "1 200 process txn 2 query 2 \n"
      "1 200 process txn 2 query 3 \n"
      "1 200 commit txn 2 \n"
      "1 100 rollback txn 1 \n"
      "2 100 response txn 1 \n"
      "3 0 stop the server"
  );

  std::cin.rdbuf(stream.rdbuf());

  // Start traffic cop
  scheduler::TrafficCop::GetInstance().Execute();
}

TEST(SchedulerTests, KernelTest) {
  ResultType (*kernel_func)(const char*) = &backend::Kernel::Handler;

  scheduler::Scheduler::GetInstance().AddTask((ResultType (*)(void*)) kernel_func,
                                              (void *)
                                              "CREATE DATABASE TEST;"
                                              "CREATE TABLE T (T_ID INTEGER NOT NULL,"
                                              "  DESCRIPTION VARCHAR(300),"
                                              "  PRIMARY KEY (T_ID)"
                                              ");"
                                              "CREATE TABLE T (T_ID INTEGER NOT NULL,"
                                              "  DESCRIPTION VARCHAR(300),"
                                              "  PRIMARY KEY (TX_ID)"
                                              ");"
                                              "CREATE TABLE IF NOT EXISTS T (T_ID INTEGER NOT NULL,"
                                              "  DESCRIPTION VARCHAR(300),"
                                              "  PRIMARY KEY (T_ID)"
                                              ");"
                                              "CREATE TABLE ACCESS_INFO ("
                                              " s_id INTEGER NOT NULL,"
                                              " ai_type TINYINT NOT NULL,"
                                              " PRIMARY KEY (s_id, ai_type),"
                                              " FOREIGN KEY (s_id) REFERENCES T (T_ID)"
                                              " );"
                                              "CREATE TABLE ACCESS_INFO ("
                                              " s_id INTEGER NOT NULL,"
                                              " ai_type TINYINT NOT NULL,"
                                              " PRIMARY KEY (s_id, ait_type),"
                                              " FOREIGN KEY (sx_id) REFERENCES SUBSCRIBER (s_id)"
                                              " );"
                                              "DROP TABLE ACCESS_INFO;"
                                              "CREATE DATABASE X;"
                                              "DROP DATABASE X;"
                                              "DROP TABLE T;"
                                              "DROP TABLE T;"
                                              );

  /*
  "CREATE INDEX T_INDEX ON T (T_ID, DESCRIPTION);"
  "CREATE INDEX T_INDEX_2 ON T (DETION);"
  "CREATE INDEX XYZ_INDEX ON T ( DESCRIPTION );"
  "DROP INDEX T_INDEX ON T;"
  */

  // final wait
  scheduler::Scheduler::GetInstance().Wait();

}

} // End test namespace
} // End peloton namespace

