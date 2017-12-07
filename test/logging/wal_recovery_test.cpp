//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simple_query_test.cpp
//
// Identification: test/network/simple_query_test.cpp
//
// Copyright (c) 2016-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"
#include "common/logger.h"
#include "network/network_manager.h"
#include "network/protocol_handler_factory.h"
#include "catalog/catalog.h"
#include "catalog/manager.h"
#include "util/string_util.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/index_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "logging/wal_recovery.h"
#include "logging/logging_util.h"
#include "settings/settings_manager.h"
#include "settings/setting_id.h"

#include <pqxx/pqxx> /* libpqxx is used to instantiate C++ client */
#include <include/network/postgres_protocol_handler.h>

#define NUM_THREADS 1

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Simple Query Tests
//===--------------------------------------------------------------------===//

class RecoveryTests : public PelotonTest {};

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


void *RecoveryTest(int port) {
  try {
    logging::WalRecovery rec(0,"");
    FileHandle f;
    logging::LoggingUtil::OpenFile("logging_samples/logfile_insert", "rb", f);
    rec.RecoveryTest(f);
    // forcing the factory to generate psql protocol handler
    pqxx::connection C(StringUtil::Format(
        "host=127.0.0.1 port=%d user=postgres sslmode=disable application_name=psql", port));
    peloton::network::NetworkConnection *conn =
        peloton::network::NetworkManager::GetConnection(
            peloton::network::NetworkManager::recent_connfd);

    network::PostgresProtocolHandler *handler =
        dynamic_cast<network::PostgresProtocolHandler*>(conn->protocol_handler_.get());
    EXPECT_NE(handler, nullptr);

    pqxx::work txn2(C);
    pqxx::result R = txn2.exec("SELECT name FROM employee where id=1;");
    txn2.commit();

    EXPECT_EQ(R.size(), 1);
  } catch (const std::exception &e) {
    LOG_INFO("[SimpleQueryTest] Exception occurred: %s", e.what());
    EXPECT_TRUE(false);
  }

  LOG_INFO("[SimpleQueryTest] Client has closed");
  return NULL;
}




/**
 * Use std::thread to initiate peloton server and pqxx client in separate
 * threads
 * Simple query test to guarantee both sides run correctly
 * Callback method to close server after client finishes
 */
//TEST_F(RecoveryTests, SetupQueryTest) {
//  peloton::PelotonInit::Initialize();
//  LOG_INFO("Server initialized");
//  peloton::network::NetworkManager network_manager;

//  int port = 15721;
//  std::thread serverThread(LaunchServer, network_manager, port);
//  while (!network_manager.GetIsStarted()) {
//    sleep(1);
//  }

//  // server & client running correctly
//  StartTest(port);

//  network_manager.CloseServer();
//  serverThread.join();
//  LOG_INFO("Peloton is shutting down");
//  peloton::PelotonInit::Shutdown();
//  LOG_INFO("Peloton has shut down");
//}

TEST_F(RecoveryTests, RecoveryQueryTest) {
  settings::SettingsManager::SetBool(settings::SettingId::recovery, false);
  peloton::PelotonInit::Initialize();
  LOG_INFO("Server initialized");
  peloton::network::NetworkManager network_manager;
  //delete catalog::Manager::GetInstance();
  int port = 15721;
  std::thread serverThread(LaunchServer, network_manager, port);
  while (!network_manager.GetIsStarted()) {
    sleep(1);
  }

  // server & client running correctly
  RecoveryTest(port);

  network_manager.CloseServer();
  serverThread.join();
  LOG_INFO("Peloton is shutting down");
  peloton::PelotonInit::Shutdown();
  LOG_INFO("Peloton has shut down");
}


}  // namespace test
}  // namespace peloton
