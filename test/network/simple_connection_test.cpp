//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simple_connection_test.cpp
//
// Identification: test/network/simple_connection_test.cpp
//
// Copyright (c) 2016-17, Carnegie Mellon University Database Group
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

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Simple Connection Tests
//===--------------------------------------------------------------------===//

class SimpleConnectionTests : public PelotonTest {};

/**
 * Connect to an inexistent database
 */
void *ConnectionDatabaseTest(int port) {
  try {
    // forcing the factory to generate psql protocol handler
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=foo sslmode=disable application_name=psql", port));

    peloton::network::ConnectionHandle *conn =
        peloton::network::ConnectionHandleFactory::GetInstance().ConnectionHandleAt(
            peloton::network::PelotonServer::recent_connfd).get();

    network::PostgresProtocolHandler *handler =
        dynamic_cast<network::PostgresProtocolHandler*>(conn->GetProtocolHandler().get());
    EXPECT_NE(handler, nullptr);

    EXPECT_TRUE(false);

  } catch (const std::exception &e) {
    auto msg = e.what();
    LOG_INFO("[SimpleConnectionTest] Exception occurred: %s", msg);
    EXPECT_TRUE(true);
  }


  // forcing the factory to generate psql protocol handler
  pqxx::connection C(StringUtil::Format(
       "host=127.0.0.1 port=%d user=default_database sslmode=disable application_name=psql", port));

  peloton::network::ConnectionHandle *conn =
      peloton::network::ConnectionHandleFactory::GetInstance().ConnectionHandleAt(
          peloton::network::PelotonServer::recent_connfd).get();

  network::PostgresProtocolHandler *handler =
      dynamic_cast<network::PostgresProtocolHandler*>(conn->GetProtocolHandler().get());
  EXPECT_NE(handler, nullptr);

  LOG_INFO("[SimpleConnectionTest] Client has closed");
  return NULL;
}

/**
 * Use std::thread to initiate peloton server and pqxx client in separate
 * threads
 * Simple query test to guarantee both sides run correctly
 * Callback method to close server after client finishes
 */
TEST_F(SimpleConnectionTests, SimpleConnectionTest) {
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
  ConnectionDatabaseTest(port);

  server.Close();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}

}  // namespace test
}  // namespace peloton
