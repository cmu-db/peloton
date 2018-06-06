//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// select_all_test.cpp
//
// Identification: test/network/select_all_test.cpp
//
// Copyright (c) 2016-17, Carnegie Mellon University Database Group
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
// Select All Tests
//===--------------------------------------------------------------------===//

class SelectAllTests : public PelotonTest {};

/**
 * Select All Test
 * In this test, peloton will return the result that exceeds the 8192 bytes limits.
 * The response will be put into multiple packets and be sent back to clients.
 */
void *SelectAllTest(int port) {
  try {
    // forcing the factory to generate psql protocol handler
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=default_database sslmode=disable application_name=psql", port));
    pqxx::work txn1(C);
    peloton::network::ConnectionHandle *conn =
        peloton::network::ConnectionHandleFactory::GetInstance().ConnectionHandleAt(
            peloton::network::PelotonServer::recent_connfd).get();

    network::PostgresProtocolHandler *handler =
        dynamic_cast<network::PostgresProtocolHandler *>(conn->GetProtocolHandler().get());
    EXPECT_NE(handler, nullptr);

    // create table and insert some data
    txn1.exec("DROP TABLE IF EXISTS template;");
    txn1.exec("CREATE TABLE template(id INT);");
    txn1.commit();

    pqxx::work txn2(C);
    for (int i = 0; i < 2000; i++) {
      std::string s = "INSERT INTO template VALUES (" + std::to_string(i) + ")";
      LOG_TRACE("Start sending query");
      txn2.exec(s);
    }

    pqxx::result R = txn2.exec("SELECT * from template;");
    txn2.commit();
    EXPECT_EQ(R.size(), 2000);
  } catch (const std::exception &e) {
    LOG_INFO("[SelectAllTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }

  LOG_INFO("[SelectAllTest] Client has closed");
  return NULL;
}

TEST_F(SelectAllTests, SelectAllTest) {
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
  SelectAllTest(port);

  server.Close();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

} // namespace test
} // namespace peloton
