//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logging_test.cpp
//
// Identification: test/logging/logging_test.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "logging/log_buffer.h"
#include "common/harness.h"
#include "gtest/gtest.h"
#include "network/peloton_server.h"
#include "network/postgres_protocol_handler.h"
#include "util/string_util.h"
#include "network/connection_handle_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Tests
//===--------------------------------------------------------------------===//

class LoggingTests : public PelotonTest {};

void *LoggingTest(int port) {
  try {
    // forcing the factory to generate jdbc protocol handler
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=default_database sslmode=disable", port));
    LOG_INFO("[LoggingTest] Connected to %s", C.dbname());
    pqxx::work txn1(C);

    peloton::network::ConnectionHandle *conn =
        peloton::network::ConnectionHandleFactory::GetInstance().ConnectionHandleAt(
            peloton::network::PelotonServer::recent_connfd).get();

    //Check type of protocol handler
    network::PostgresProtocolHandler* handler =
        dynamic_cast<network::PostgresProtocolHandler*>(conn->GetProtocolHandler().get());

    EXPECT_NE(handler, nullptr);

    // create table and insert some data
    txn1.exec("DROP TABLE IF EXISTS employee;");
    txn1.exec("CREATE TABLE employee(id INT, name VARCHAR(100));");
    txn1.commit();

    pqxx::work txn2(C);
    txn2.exec("INSERT INTO employee VALUES (1, 'Aaron Tian');");
    txn2.exec("INSERT INTO employee VALUES (2, 'Gandeevan Raghuraman');");
    txn2.exec("INSERT INTO employee VALUES (3, 'Anirudh Kanjani');");
    txn2.commit();

  } catch (const std::exception &e) {
    LOG_INFO("[LoggingTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }
  return NULL;
}

TEST_F(LoggingTests, LoggingTest) {
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
  LoggingTest(port);
  server.Close();
  serverThread.join();
  peloton::PelotonInit::Shutdown();
  LOG_DEBUG("Peloton has shut down");

}
}
}