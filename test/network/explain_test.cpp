//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// explain_test.cpp
//
// Identification: test/network/explain_test.cpp
//
// Copyright (c) 2016-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"
#include "common/logger.h"
#include "network/peloton_server.h"
#include "network/protocol_handler_factory.h"
#include "network/connection_handle_factory.h"
#include "util/string_util.h"
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "network/postgres_protocol_handler.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Explain Tests
//===--------------------------------------------------------------------===//

class ExplainTests : public PelotonTest {};

/**
 * Explain Test
 * Test how the network layer handle explain
 */
void *ExplainTest(int port) {
  try {
    // forcing the factory to generate psql protocol handler
    pqxx::connection C(
        StringUtil::Format("host=127.0.0.1 port=%d user=default_database "
                           "sslmode=disable application_name=psql",
                           port));
    pqxx::work txn1(C);
    peloton::network::ConnectionHandle *conn =
        peloton::network::ConnectionHandleFactory::GetInstance()
            .ConnectionHandleAt(peloton::network::PelotonServer::recent_connfd)
            .get();

    network::PostgresProtocolHandler *handler =
        dynamic_cast<network::PostgresProtocolHandler *>(
            conn->GetProtocolHandler().get());
    EXPECT_NE(handler, nullptr);

    // create table and insert some data
    txn1.exec("DROP TABLE IF EXISTS template;");
    txn1.exec("CREATE TABLE template(id INT);");
    txn1.commit();

    // Execute EXPLAIN directly
    pqxx::work txn2(C);
    pqxx::result result1 = txn2.exec("EXPLAIN SELECT * from template;");
    txn2.commit();
    EXPECT_EQ(result1.size(), 1);

    // Execute EXPLAIN through PREPARE statement
    pqxx::work txn3(C);
    txn3.exec("PREPARE func AS EXPLAIN SELECT * from template;");
    pqxx::result result2 = txn3.exec("EXECUTE func");
    txn3.commit();
    EXPECT_EQ(result2.size(), 1);
  } catch (const std::exception &e) {
    LOG_INFO("[ExplainTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }

  LOG_INFO("[ExplainTest] Client has closed");
  return NULL;
}

TEST_F(ExplainTests, ExplainTest) {
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
  ExplainTest(port);

  server.Close();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

}  // namespace test
}  // namespace peloton
