//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// libpqxx_test.cpp
//
// Identification: test/network/libpqxx_test.cpp
//
// Copyright (c) 2016-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <common/harness.h>
#include "network/network_manager.h"
#include "network/protocol_handler_factory.h"
#include <include/network/postgres_protocol_handler.h>
#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Libpqxx Tests
//===--------------------------------------------------------------------===//
class LibpqxxTests : public PelotonTest {};

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
 * Libpqxx pipeline mode test
 * When using pipeline, all the queries in the pipeline are sent in one huge packet.
 * Queries are not splitted by command messages.
 */

void *PipelineTest(int port) {
  try {
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=postgres sslmode=disable application_name=psql", port));

    peloton::network::NetworkConnection *conn =
        peloton::network::NetworkManager::GetConnection(
            peloton::network::NetworkManager::recent_connfd);
    network::PostgresProtocolHandler *handler =
        dynamic_cast<network::PostgresProtocolHandler*>(conn->protocol_handler_.get());
    EXPECT_NE(handler, nullptr);

    pqxx::work txn1(C);
    pqxx::pipeline pipeline(txn1);
    pipeline.retain(10);
    int i = 0;
    pipeline.insert("drop table if exists goo");
    pipeline.insert("create table goo(id integer)");
    for (; i < 10; i++) {
      pipeline.insert("insert into goo values(1)");
    }
    pipeline.complete();
    txn1.commit();

    pqxx::work txn2(C);
    pqxx::result R = txn2.exec("SELECT * FROM goo");
    txn2.commit();
    EXPECT_EQ(R.size(), 10);
  } catch (const std::exception &e) {
    LOG_INFO("[LibpqxxPipelineTest] Exception occured: %s", e.what());
    EXPECT_TRUE(false);
  }
  LOG_INFO("[LibpqxxPipelineTest] Client has closed");
  return NULL;
}

TEST_F(LibpqxxTests, PipelineTest) {
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::network::NetworkManager network_manager;

  int port = 15721;
  std::thread serverThread(LaunchServer, network_manager, port);
  while (!network_manager.GetIsStarted()) {
    sleep(1);
  }

  // server & client running correctly
  // TODO: Right now, it's not working correctly.
//  PipelineTest(port);

  network_manager.CloseServer();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}
} //namespace test
} //namespace peloton