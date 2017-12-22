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
#include "network/network_manager.h"
#include "network/postgres_protocol_handler.h"
#include "util/string_util.h"

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Prepare Stmt Tests
//===--------------------------------------------------------------------===//

class PrepareStmtTests : public PelotonTest {};

static void *LaunchServer(peloton::network::NetworkManager network_manager,
                          int port) {
  try {
    network_manager.SetPort(port);
    network_manager.StartServer();
  } catch (peloton::ConnectionException exception) {
    LOG_INFO("[LaunchServer] exception in thread");
  }
  return NULL;
}

/**
 * named prepare statement without parameters
 * TODO: add prepare's parameters when parser team fix the bug
 */

void *PrepareStatementTest(int port) {
  try {
    // forcing the factory to generate jdbc protocol handler
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=postgres sslmode=disable", port));
    LOG_INFO("[PrepareStatementTest] Connected to %s", C.dbname());
    pqxx::work txn1(C);

    peloton::network::NetworkConnection *conn =
        peloton::network::NetworkManager::GetConnection(
            peloton::network::NetworkManager::recent_connfd);

    //Check type of protocol handler
    network::PostgresProtocolHandler* handler =
        dynamic_cast<network::PostgresProtocolHandler*>(conn->protocol_handler_.get());

    EXPECT_NE(handler, nullptr);

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
  peloton::network::NetworkManager network_manager;
  int port = 15721;
  std::thread serverThread(LaunchServer, network_manager, port);
  while (!network_manager.GetIsStarted()) {
    sleep(1);
  }

  PrepareStatementTest(port);
  LOG_DEBUG("Server Closing");
  network_manager.CloseServer();
  serverThread.join();
  peloton::PelotonInit::Shutdown();
  LOG_DEBUG("Peloton has shut down");
}

}  // namespace test
}  // namespace peloton
