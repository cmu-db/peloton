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
#include "network/network_manager.h"
#include "network/protocol_handler_factory.h"
#include "util/string_util.h"
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include "network/postgres_protocol_handler.h"

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Select All Tests
//===--------------------------------------------------------------------===//

class SelectAllTests : public PelotonTest {};

static void *LaunchServer(peloton::network::NetworkManager network_manager,
                          int port) {
  try {
    network_manager.SetPort(port);
    network_manager.StartServer();
  } catch (peloton::ConnectionException &exception) {
    LOG_INFO("[LaunchServer] exception in thread");
  }
  return NULL;
}

/**
 * Select All Test
 * In this test, peloton will return the result that exceeds the 8192 bytes limits.
 * The response will be put into multiple packets and be sent back to clients.
 */
void *SelectAllTest(int port) {
  try {
    // forcing the factory to generate psql protocol handler
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=postgres sslmode=disable application_name=psql", port));
    pqxx::work txn1(C);

    peloton::network::NetworkConnection *conn =
        peloton::network::NetworkManager::GetConnection(
            peloton::network::NetworkManager::recent_connfd);

    network::PostgresProtocolHandler *handler =
        dynamic_cast<network::PostgresProtocolHandler *>(conn->protocol_handler_.get());
    EXPECT_NE(handler, nullptr);

    // create table and insert some data
    txn1.exec("DROP TABLE IF EXISTS template;");
    txn1.exec("CREATE TABLE template(id INT);");
    txn1.commit();

    pqxx::work txn2(C);
    for (int i = 0; i < 2000; i++) {
      std::string s = "INSERT INTO template VALUES (" + std::to_string(i) + ")";
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
  peloton::network::NetworkManager network_manager;

  int port = 15721;
  std::thread serverThread(LaunchServer, network_manager, port);
  while (!network_manager.GetIsStarted()) {
    sleep(1);
  }

  // server & client running correctly
  SelectAllTest(port);

  network_manager.CloseServer();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

} // namespace test
} // namespace peloton
