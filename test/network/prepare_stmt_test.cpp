//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// prepare_stmt_test.cpp
//
// Identification: test/network/prepare_stmt_test.cpp
//
// Copyright (c) 2016-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "common/harness.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "network/peloton_server.h"
#include "network/postgres_protocol_handler.h"
#include "util/string_util.h"
#include "network/network_io_wrapper_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Prepare Stmt Tests
//===--------------------------------------------------------------------===//

class PrepareStmtTests : public PelotonTest {};

/**
 * named prepare statement without parameters
 * TODO: add prepare's parameters when parser team fix the bug
 */

void *PrepareStatementTest(int port) {
  try {
    // forcing the factory to generate jdbc protocol handler
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=default_database sslmode=disable", port));
    LOG_INFO("[PrepareStatementTest] Connected to %s", C.dbname());
    pqxx::work txn1(C);

    // create table and insert some data
    txn1.exec("DROP TABLE IF EXISTS employee;");
    txn1.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
    txn1.commit();

    pqxx::work txn2(C);
    txn2.exec("INSERT INTO employee VALUES (1, 'Han LI');");
    txn2.exec("INSERT INTO employee VALUES (2, 'Shaokun ZOU');");
    txn2.exec("INSERT INTO employee VALUES (3, 'Yilei CHU');");

    // test prepare statement
    C.prepare("searchstmt", "SELECT name FROM employee WHERE id=$1;");
    // invocation as in variable binding
    pqxx::result R = txn2.prepared("searchstmt")(1).exec();
    txn2.commit();

    // test prepared statement already in statement cache
    // LOG_INFO("[Prepare statement cache]
    // %d",conn->protocol_handler_.ExistCachedStatement("searchstmt"));
    EXPECT_EQ(R.size(), 1);

  } catch (const std::exception &e) {
    LOG_INFO("[PrepareStatementTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }
  return NULL;
}

TEST_F(PrepareStmtTests, PrepareStatementTest) {

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
  PrepareStatementTest(port);
  server.Close();
  serverThread.join();
  peloton::PelotonInit::Shutdown();
  LOG_DEBUG("Peloton has shut down");
}

}  // namespace test
}  // namespace peloton
