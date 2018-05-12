//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// psql_temp_table_test.cpp
//
// Identification: test/catalog/psql_temp_table_test.cpp
//
// Copyright (c) 2016-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"
#include "common/logger.h"
#include "network/peloton_server.h"
#include "network/protocol_handler_factory.h"
#include "util/string_util.h"
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "network/postgres_protocol_handler.h"
#include "network/connection_handle_factory.h"
#include "parser/postgresparser.h"
#include "concurrency/transaction_manager_factory.h"
#include "catalog/catalog.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Psql temporary table tests
//===--------------------------------------------------------------------===//

class PsqlTempTableTests : public PelotonTest {};

/**
 * Table visibility test - check:
 *   1. a temp table makes the permanent table with the same name invisible
 *   2. a temp table created by one session is invisible to another session
 */
void *TableVisibilityTest(int port) {
  // Set up connection 1
  pqxx::connection C1(StringUtil::Format(
      "host=127.0.0.1 port=%d user=default_database sslmode=disable "
      "application_name=psql",
      port));

  // Connection 1 creates a permanent table, which has one tuple
  pqxx::work txn11(C1);
  txn11.exec("DROP TABLE IF EXISTS employee;");
  txn11.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
  txn11.exec("INSERT INTO employee VALUES(1, 'trump');");

  // Connection 1 creates a temp table with the same name, which has 2 tuples
  txn11.exec("CREATE TEMP TABLE employee(id INT, name VARCHAR(100));");
  txn11.exec("INSERT INTO employee VALUES(1, 'trump');");
  txn11.exec("INSERT INTO employee VALUES(2, 'trumpet');");
  txn11.commit();

  // Temp table should make permanent table invisible
  pqxx::work txn12(C1);
  pqxx::result R12 = txn12.exec("select * from employee;");
  EXPECT_EQ(R12.size(), 2);
  txn12.commit();

  // Set up connection 2
  // The table visible to connection 2 should be the permanent table
  pqxx::connection C2(StringUtil::Format(
      "host=127.0.0.1 port=%d user=default_database sslmode=disable "
      "application_name=psql",
      port));
  pqxx::work txn21(C2);
  pqxx::result R21 = txn21.exec("select * from employee;");
  EXPECT_EQ(R21.size(), 1);
  txn21.commit();

  // Connection 2 drops the permanent table
  pqxx::work txn22(C2);
  txn22.exec("drop table employee;");
  txn22.commit();

  // Now no table is visible to connection 2. (The temp table created by
  // connection 1 is invisible.)
  // Expect an exception: "Table employee is not found"
  pqxx::work txn23(C2);
  try {
    txn23.exec("select * from employee;");
    EXPECT_TRUE(false);
  } catch (const std::exception &e) {
    LOG_INFO("[TableVisibilityTest] Exception occurred (as expected): %s",
             e.what());
  }
  C2.disconnect();

  // Connection 1 can still see its temp table
  pqxx::work txn13(C1);
  pqxx::result R13 = txn13.exec("select * from employee;");
  EXPECT_EQ(R13.size(), 2);
  txn13.commit();

  // Connection 1 drops its temp table
  pqxx::work txn14(C1);
  txn14.exec("drop table employee;");
  txn14.commit();

  // Now no table is visible to connection 1
  // Expect an exception: "Table employee is not found"
  pqxx::work txn15(C1);
  try {
    txn15.exec("select * from employee;");
    EXPECT_TRUE(false);
  } catch (const std::exception &e) {
    LOG_INFO("[TableVisibilityTest] Exception occurred (as expected): %s",
             e.what());
  }
  C1.disconnect();

  LOG_INFO("[TableVisibilityTest] Client has closed");
  return NULL;
}

TEST_F(PsqlTempTableTests, TableVisibilityTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::network::PelotonServer server;

  int port = 15721;
  try {
    server.SetPort(port);
    server.SetupServer();
  } catch (peloton::ConnectionException &exception) {
    LOG_INFO("[LaunchServer] exception when launching server");
  }
  std::thread serverThread([&]() { server.ServerLoop(); });

  // server & client running correctly
  TableVisibilityTest(port);

  server.Close();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

}  // namespace test
}  // namespace peloton